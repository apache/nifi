/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as d3 from 'd3';
import { Position } from '@nifi/shared';
import { CanvasConnection, CanvasDatum, CanvasRootResolver, CanvasRootSelection } from '../canvas.types';
import { CanvasConstants } from '../canvas.constants';
import { LayerDragEndCallback } from '../layers/render-context.types';

/** Positionable canvas wrappers (everything except connections). */
type CanvasPositionableDatum = Exclude<CanvasDatum, CanvasConnection>;

/**
 * Minimal structural shape `attachComponentDrag` requires of a per-component
 * datum. Every per-renderer `CanvasXxx` datum satisfies this implicitly because
 * its `ui` field extends `DragUiState` (defined in `canvas.types.ts`, carrying
 * `dragDelta` / `dragMovingIds` / etc.) and its `entity` carries the canonical
 * id and permissions. The structural shape is intentionally narrow — the
 * renderer-specific per-type ui state (`componentType`, `dimensions`,
 * `currentPosition`, etc.) is not declared here so each renderer keeps its
 * stricter typed datum at the call site.
 */
export interface DraggableDatum {
    entity: {
        id: string;
        permissions: { canRead: boolean; canWrite: boolean };
    };
    ui: {
        dragDelta?: Position;
        dragMovingIds?: Set<string>;
    };
}

/**
 * Configuration for `DragUtils.attachComponentDrag`. Each getter is read at
 * fire-time so the gesture honors runtime toggles of `canEdit`, `canSelect`,
 * the disabled-during-save set, and the canonical selection — without
 * requiring the drag chain to be re-attached.
 */
export interface AttachComponentDragOptions<TDatum extends DraggableDatum> {
    /**
     * Resolver for the host canvas's `g.canvas` root. Invoked at the start of
     * every gesture so `g.component` / `g.connection` queries are scoped to a
     * single canvas instance (two canvases on the same page never cross-talk
     * via global selectors). Mirrors the contract used by
     * `ConnectableBehaviorHelper` so both interactions share one source of
     * truth for "which canvas am I in" — see `CanvasRootResolver` in
     * `canvas.types.ts`.
     */
    resolveCanvasRoot: CanvasRootResolver;
    /** Read at fire-time to gate the drag filter on edit permission. */
    getCanEdit: () => boolean;
    /** Read at fire-time to gate the drag filter on selection mode. */
    getCanSelect: () => boolean;
    /** Read at fire-time to gate the drag filter on per-id saving state. */
    getDisabledIds: () => Set<string>;
    /** Read at the start of every gesture to compute the canonical `dragMovingIds` snapshot. */
    getSelectedIds: () => Set<string>;
    /** Required drag-end callback. Always invoked on gesture end, even at zero delta. */
    onDragEnd: LayerDragEndCallback;
    /**
     * Optional fire-time predicate AND-ed with the standard checks (button,
     * ctrl-modifier, canEdit, canSelect, disabled). Used by `LabelRenderer`
     * to restrict drags to the body rect (the resize triangle has its own
     * drag attached separately). Returning `false` rejects the gesture before
     * `start` fires.
     */
    extraFilter?: (event: MouseEvent, datum: TDatum) => boolean;
}

/**
 * Shared drag helper used by every per-type renderer's `attachDragBehavior`. A drag
 * gesture is uniformly handled here whether it moves 1 component or N: each renderer's
 * `start` builds the moving-set on the canonical `selectedIds` signal, stashes it on
 * `d.ui.dragMovingIds`, and then delegates the visual translation, connection re-pathing
 * (component- AND PG/RPG-anchored), and self-loop bend translation to the methods below.
 *
 * The grabbed datum carries the per-gesture state on its `ui` field so this utility can
 * stay stateless:
 *   - `d.ui.dragMovingIds` — the set of entity IDs (components + selected connections)
 *     that participate in this gesture. The source-of-truth for "what's moving."
 *   - `d.ui.dragDelta` — the running delta accumulator mutated each `drag` tick.
 *
 * Reading `d.ui.dragMovingIds` instead of walking `g.component.selected` matters for the
 * unselected-grab case: when a user grabs an unselected component while a multi-selection
 * exists, the renderer's `mousedown.selection` handler updates the canonical `selectedIds`
 * signal before `start` fires, but the DOM `selected` class is reapplied by an Angular
 * effect that has not run yet by the time `beginDrag` is called. Trusting the DOM class
 * would erroneously include the abandoned multi-selection in the gesture; trusting
 * `dragMovingIds` (computed from the canonical signal) gives the right answer.
 *
 * Every method takes a `resolveCanvasRoot` callback so its `selectAll` queries are
 * scoped to a single canvas instance, not the whole document. The resolver is
 * invoked per-gesture (not cached at attach-time) so a teardown / re-mount of the
 * host canvas in dev mode never leaves the drag pipeline pointing at a stale `<g>`.
 * See `CanvasRootResolver` in `canvas.types.ts`.
 */
export class DragUtils {
    /**
     * Custom property name used to stamp the active drag's moving-id set onto
     * the canvas root so per-PG `mouseover.drop` handlers can decide whether
     * the hovered PG is excluded from the drop-target affordance. Property
     * (not datum) so a future caller that binds a datum to `g.canvas` does
     * not collide with this gesture state.
     */
    private static readonly MOVING_IDS_PROPERTY = '__dragMovingIds__';

    /**
     * Class set on the canvas root for the duration of a component drag.
     * The PG renderer's `mouseover.drop` uses this as the "is a component
     * drag in progress?" signal. Mirrors flow-designer's
     * `rect.drag-selection`-presence gate at
     * `process-group-manager.service.ts:161`, except we use a class on
     * `g.canvas` instead of the existence of a sibling DOM rectangle —
     * the reusable canvas does not produce flow-designer's
     * `rect.drag-selection` element.
     */
    public static readonly DRAGGING_CLASS = 'is-component-dragging';

    /**
     * Drop-target affordance class applied to the unique `g.process-group`
     * the user is hovering over during an active component drag. Styled in
     * `canvas.component.scss`. Set/cleared by the PG renderer's
     * `mouseover.drop` / `mouseout.drop` handlers, and unconditionally
     * cleared by {@link endCanvasDrag} as a drag-cancel safety net.
     */
    public static readonly DROP_CLASS = 'drop';

    /**
     * Class applied to the dashed bounding rectangle that follows the cursor
     * during a component drag. Mirrors flow-designer's `rect.drag-selection`
     * (see `draggable-behavior.service.ts`). Always exactly zero or one of
     * these elements exists at a time — there is at most one active drag per
     * canvas, and the gesture lifecycle creates / removes the element on
     * `start` / `end`.
     */
    public static readonly DRAG_SELECTION_CLASS = 'drag-selection';

    /**
     * Padding (in canvas units) applied to every side of the moving-set's
     * bounding box when the drag-selection rect is created. Keeps the dashed
     * outline from sitting flush against the components it represents — small
     * enough that it stays a recognizable "selection rectangle" rather than a
     * loose container.
     */
    private static readonly DRAG_SELECTION_PADDING = 4;

    /**
     * Stamp the canvas root with the active drag's moving-id set and the
     * `is-component-dragging` class. Called from {@link attachComponentDrag}'s
     * `start` handler after `beginDrag` runs. The set is read at fire-time by
     * each PG's `mouseover.drop` to decide whether the hovered PG is excluded
     * from the drop affordance (self-drop guard).
     *
     * No-op when `movingIds` is empty; that path corresponds to a degenerate
     * gesture that is going to be aborted by `finalizeDrag` anyway.
     *
     * The stamped value is a defensive snapshot (`new Set(movingIds)`) so a
     * caller mutating its own set after the call cannot retroactively change
     * the answers returned by `getActiveDragMovingIds` mid-gesture. No current
     * caller mutates the input, but the snapshot makes that contract explicit.
     */
    public static beginCanvasDrag(canvasGroup: CanvasRootSelection, movingIds: Set<string>): void {
        if (movingIds.size === 0) {
            return;
        }
        canvasGroup.classed(DragUtils.DRAGGING_CLASS, true);
        canvasGroup.property(DragUtils.MOVING_IDS_PROPERTY, new Set(movingIds));
    }

    /**
     * Clear the canvas-root drag stamps and any lingering `g.process-group.drop`
     * affordance. Called from {@link attachComponentDrag}'s `end` handler.
     *
     * The `selectAll('g.process-group.drop').classed('drop', false)` sweep is a
     * defensive safety net: under normal use `mouseout.drop` already clears
     * `.drop` on hover-leave, but if the cursor exits the canvas via the root
     * (no PG mouseout fires) we would otherwise leak the affordance.
     */
    public static endCanvasDrag(canvasGroup: CanvasRootSelection): void {
        canvasGroup.classed(DragUtils.DRAGGING_CLASS, false);
        canvasGroup.property(DragUtils.MOVING_IDS_PROPERTY, null);
        canvasGroup
            .selectAll<SVGGElement, CanvasDatum>(`g.process-group.${DragUtils.DROP_CLASS}`)
            .classed(DragUtils.DROP_CLASS, false);
    }

    /**
     * Read the moving-id set stamped by {@link beginCanvasDrag}, or `null` if
     * no drag is active. Consumed by the PG renderer's `mouseover.drop`.
     */
    public static getActiveDragMovingIds(canvasGroup: CanvasRootSelection): Set<string> | null {
        const movingIds = canvasGroup.property(DragUtils.MOVING_IDS_PROPERTY) as Set<string> | null | undefined;
        return movingIds ?? null;
    }

    /**
     * Read the unique drop-target PG's entity id from the canvas, or `null` if
     * none is set. Single source of truth for "which PG is the current drop
     * target?" — called once at drag-end from `canvas.component.ts:onDragEnd`
     * to project `targetGroupId` onto the `componentsDragEnd` payload.
     *
     * Returns `null` when no `g.process-group.drop` element exists (the user
     * dropped on empty canvas, or the toggle pipeline correctly excluded the
     * hovered PG via the self-drop guard). If multiple `.drop` elements
     * somehow coexist (a bug elsewhere in the toggle pipeline), only the
     * first match is returned; treating the situation as ambiguous would
     * make consumers harder to reason about.
     */
    public static readDropTarget(canvasGroup: CanvasRootSelection): string | null {
        const drop = canvasGroup.select<SVGGElement>(`g.process-group.${DragUtils.DROP_CLASS}`);
        if (drop.empty()) {
            return null;
        }
        const datum = drop.datum() as { entity?: { id?: string } } | undefined;
        return datum?.entity?.id ?? null;
    }

    /**
     * Append `<rect class="drag-selection">` under the canvas root sized to
     * the bounding box of every moving entity. The rect has `pointer-events:
     * none` (set both as an SVG attribute and via CSS) so its presence never
     * intercepts `mouseover` events headed for an underlying PG — that is the
     * whole reason this model is preferable to per-tick component
     * translation, which sat the dragged component on top of the cursor and
     * blocked drop-target hit-testing.
     *
     * The bbox spans:
     *   1. Every `g.component` whose entity id is in `movingIds`, using
     *      `entity.position` + `ui.dimensions` to derive each component's
     *      world-space rectangle.
     *   2. Every `g.connection` whose bends should travel with the gesture —
     *      selected connections (id in `movingIds`) and self-loops on a
     *      moving component (sourceId === destinationId AND sourceId in
     *      movingIds). Bends contribute as 0×0 points; this matches the
     *      `beginDrag` snapshot pass that decides which bends translate at
     *      drop.
     *
     * The element is bound a `{ original: {x, y}, x, y }` datum so per-tick
     * `updateDragSelectionRect` calls can accumulate `(dx, dy)` against the
     * datum (NOT the DOM attrs, which round under snap-to-grid) and so
     * `getDragSelectionDelta` can read the snap-aligned final delta at
     * gesture end.
     *
     * No-op if `movingIds` is empty or no moving entity yields a usable
     * rectangle / point — neither path corresponds to a real gesture and
     * appending a degenerate rect would just leak DOM the consumer never
     * cleans up.
     */
    public static createDragSelectionRect(canvasGroup: CanvasRootSelection, movingIds: Set<string>): void {
        if (movingIds.size === 0) {
            return;
        }
        let minX: number | null = null;
        let minY: number | null = null;
        let maxX: number | null = null;
        let maxY: number | null = null;
        const accumulatePoint = (x: number, y: number): void => {
            if (!Number.isFinite(x) || !Number.isFinite(y)) {
                return;
            }
            if (minX === null || x < minX) minX = x;
            if (minY === null || y < minY) minY = y;
            if (maxX === null || x > maxX) maxX = x;
            if (maxY === null || y > maxY) maxY = y;
        };

        canvasGroup.selectAll<SVGGElement, CanvasPositionableDatum>('g.component').each(function (
            datum: CanvasPositionableDatum
        ) {
            const id = datum?.entity?.id;
            if (!id || !movingIds.has(id)) {
                return;
            }
            const position = datum?.entity?.position;
            const dims = datum?.ui?.dimensions;
            if (!position || !dims) {
                return;
            }
            accumulatePoint(position.x, position.y);
            accumulatePoint(position.x + (dims.width ?? 0), position.y + (dims.height ?? 0));
        });

        canvasGroup.selectAll<SVGGElement, CanvasConnection>('g.connection').each(function (datum: CanvasConnection) {
            const e = datum?.entity;
            if (!e || !Array.isArray(e.bends)) {
                return;
            }
            const isMovingConnection = movingIds.has(e.id);
            const isSelfLoopOnMoving = e.sourceId === e.destinationId && movingIds.has(e.sourceId);
            if (!isMovingConnection && !isSelfLoopOnMoving) {
                return;
            }
            for (const bend of e.bends as Position[]) {
                accumulatePoint(bend.x, bend.y);
            }
        });

        if (minX === null || minY === null || maxX === null || maxY === null) {
            return;
        }

        const padding = DragUtils.DRAG_SELECTION_PADDING;
        const minXNum = minX as number;
        const minYNum = minY as number;
        const maxXNum = maxX as number;
        const maxYNum = maxY as number;
        const x = minXNum - padding;
        const y = minYNum - padding;
        const width = maxXNum - minXNum + padding * 2;
        const height = maxYNum - minYNum + padding * 2;

        canvasGroup
            .append('rect')
            .attr('class', DragUtils.DRAG_SELECTION_CLASS)
            .attr('pointer-events', 'none')
            .attr('rx', 6)
            .attr('ry', 6)
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .datum({ original: { x, y }, x, y });
    }

    /**
     * Apply a per-tick `(dx, dy)` to the drag-selection rect's running
     * position. Mirrors flow-designer's `draggable-behavior.service.ts:124`
     * model: the unsnapped position is accumulated on the rect's datum, and
     * the snapped (or raw, when the user holds shift) position is written to
     * the rect's `x`/`y` attributes. Snap is applied at write-time so the
     * rect gracefully toggles between snapped / unsnapped within a single
     * gesture as the user presses or releases shift.
     *
     * No-op if no rect exists for `canvasGroup` — that path corresponds to a
     * `start` handler that filtered out (e.g. permissions revoked between
     * `mousedown` and the d3.drag filter), in which case there is nothing to
     * track and no gesture state to leak.
     */
    public static updateDragSelectionRect(
        canvasGroup: CanvasRootSelection,
        dx: number,
        dy: number,
        snapEnabled: boolean
    ): void {
        const rect = canvasGroup.select<SVGRectElement>(`rect.${DragUtils.DRAG_SELECTION_CLASS}`);
        if (rect.empty()) {
            return;
        }
        const datum = rect.datum() as { original: Position; x: number; y: number } | undefined;
        if (!datum) {
            return;
        }
        datum.x += dx;
        datum.y += dy;
        const displayX = snapEnabled
            ? Math.round(datum.x / CanvasConstants.SNAP_ALIGNMENT_PIXELS) * CanvasConstants.SNAP_ALIGNMENT_PIXELS
            : datum.x;
        const displayY = snapEnabled
            ? Math.round(datum.y / CanvasConstants.SNAP_ALIGNMENT_PIXELS) * CanvasConstants.SNAP_ALIGNMENT_PIXELS
            : datum.y;
        rect.attr('x', displayX).attr('y', displayY);
    }

    /**
     * Remove the drag-selection rect from `canvasGroup`. Idempotent: the
     * `selectAll(...).remove()` shape no-ops when the rect is absent. Called
     * unconditionally from `attachComponentDrag.end` so a leaked rect can
     * never persist past a gesture.
     */
    public static removeDragSelectionRect(canvasGroup: CanvasRootSelection): void {
        canvasGroup.selectAll<SVGRectElement, unknown>(`rect.${DragUtils.DRAG_SELECTION_CLASS}`).remove();
    }

    /**
     * Compute the snap-aligned final delta of the drag-selection rect — the
     * difference between its current displayed position and the position it
     * occupied at gesture start. Read at drag-end and passed to
     * {@link finalizeDrag} so the final commit shares one source of truth
     * with the per-tick visual.
     *
     * The rect's `x`/`y` attributes already hold the snapped value (see
     * `updateDragSelectionRect`), so this helper reads from the DOM attrs —
     * not from the unsnapped datum — for a final delta that already honors
     * the user's shift-state at drop.
     *
     * Returns `{ x: 0, y: 0 }` when no rect exists; that path corresponds to
     * a no-op `start` (filtered) followed by a no-op `end` and the canvas
     * component's own zero-delta short-circuit will catch it downstream.
     */
    public static getDragSelectionDelta(canvasGroup: CanvasRootSelection): Position {
        const rect = canvasGroup.select<SVGRectElement>(`rect.${DragUtils.DRAG_SELECTION_CLASS}`);
        if (rect.empty()) {
            return { x: 0, y: 0 };
        }
        const datum = rect.datum() as { original: Position; x: number; y: number } | undefined;
        if (!datum) {
            return { x: 0, y: 0 };
        }
        const displayX = parseFloat(rect.attr('x'));
        const displayY = parseFloat(rect.attr('y'));
        return {
            x: (Number.isFinite(displayX) ? displayX : datum.x) - datum.original.x,
            y: (Number.isFinite(displayY) ? displayY : datum.y) - datum.original.y
        };
    }

    /**
     * Wire the canonical component-drag pipeline onto `selection`. Captures the
     * shared shape that every per-type renderer (processor, funnel, port,
     * process-group, remote-process-group, label-position) was duplicating
     * verbatim before this helper existed: a permissions-gated `moveable`-class
     * diff for attach/detach, a fire-time-gated d3.drag chain that reads
     * canEdit / canSelect / the disabled-set / selectedIds via the supplied
     * getters, the standard dragMovingIds snapshot, and `beginDrag` /
     * `createDragSelectionRect` /
     * `updateDragSelectionRect` / `finalizeDrag` / `removeDragSelectionRect`
     * delegation through the existing `DragUtils` helpers. The dashed
     * bounding rect (mirror of flow-designer's `rect.drag-selection`) is the
     * only mid-gesture visual; components stay at their entity positions
     * until the optimistic commit at drop.
     *
     * **Attachment is gated by a `moveable` class** so the per-element listener
     * list stays stable across renders. Re-attaching `.call(drag)` on every
     * render (with a preceding `.on('.drag', null)` teardown) churns the
     * listener list and can silently suppress dispatch of the d3-installed
     * `mousedown.drag` after the same-element `mousedown.selection` handler
     * runs. By only attaching to `newlyMoveable` and detaching from
     * `noLongerMoveable`, the chain installs once per element and tears down
     * only when write permissions are revoked.
     *
     * **Why the moving-set is read from `getSelectedIds()` and stashed on
     * `d.ui.dragMovingIds` rather than walked from the DOM `g.component.selected`
     * class:** selection is owned by each renderer's `mousedown.selection`
     * handler, which updates the canonical `selectedIds` signal before `start`
     * fires. The DOM `selected` class is reapplied by an Angular effect that
     * has not run yet by the time `start` fires. Trusting the DOM class would
     * erroneously include the abandoned multi-selection in the gesture; trusting
     * `getSelectedIds()` (the same canonical signal) gives the right answer.
     * The renderer snapshots this once at `start` so `end` can hand the same set
     * to `onDragEnd` without re-reading a signal that may have moved on.
     *
     * **The drag-end callback fires unconditionally** — the canvas owns the
     * zero-delta short-circuit so all drag-end side effects (telemetry, HUD,
     * persistence) share one decision point. See `LayerDragEndCallback`.
     */
    public static attachComponentDrag<TDatum extends DraggableDatum>(
        selection: d3.Selection<SVGGElement, TDatum, d3.BaseType, unknown>,
        options: AttachComponentDragOptions<TDatum>
    ): void {
        const eligible = selection.filter((d: TDatum) => d.entity.permissions.canWrite && d.entity.permissions.canRead);
        const newlyMoveable = eligible.filter(function (this: SVGGElement) {
            return !d3.select(this as Element).classed('moveable');
        });
        const noLongerMoveable = selection.filter(function (this: SVGGElement, d: TDatum) {
            const isMoveable = d3.select(this as Element).classed('moveable');
            const stillEligible = d.entity.permissions.canWrite && d.entity.permissions.canRead;
            return isMoveable && !stillEligible;
        });
        // Detach is cheap and never needs the drag instance; run it before the
        // attach guard so revoked elements are torn down even on no-op renders.
        noLongerMoveable.classed('moveable', false).on('.drag', null);

        // Only build the d3.drag() chain when we actually have something to attach
        // it to. After steady state this branch is skipped on every render.
        if (newlyMoveable.empty()) {
            return;
        }

        const { resolveCanvasRoot, getCanEdit, getCanSelect, getDisabledIds, getSelectedIds, onDragEnd, extraFilter } =
            options;

        const drag = d3
            .drag<SVGGElement, TDatum>()
            .filter(function (event, d) {
                if (event.ctrlKey || event.button !== 0) {
                    return false;
                }
                if (!getCanEdit()) {
                    return false;
                }
                if (!getCanSelect()) {
                    return false;
                }
                if (getDisabledIds().has(d.entity.id)) {
                    return false;
                }
                if (extraFilter && !extraFilter(event, d)) {
                    return false;
                }
                return true;
            })
            .clickDistance(4)
            .on('start', function (event: d3.D3DragEvent<SVGGElement, TDatum, TDatum>, d: TDatum) {
                const selectedIds = getSelectedIds();
                const grabbedId = d.entity.id;

                // Compute the moving-set from the CANONICAL selectedIds signal
                // (NOT from g.component.selected, which lags during an
                // unselected-grab and would race with mousedown.selection).
                const movingIds =
                    selectedIds.has(grabbedId) && selectedIds.size > 1
                        ? new Set(selectedIds)
                        : new Set<string>([grabbedId]);

                event.sourceEvent.stopPropagation();

                // `dragDelta` no longer accumulates the per-tick (dx, dy) — the
                // drag-selection rect's datum owns that now. We still set this
                // to a non-null value so the `end` handler's "real gesture?"
                // gate (`if (d.ui.dragDelta)`) correctly distinguishes a
                // started gesture from a filtered one without re-querying the
                // canvas DOM.
                d.ui.dragDelta = { x: 0, y: 0 };
                d.ui.dragMovingIds = movingIds;
                // CRITICAL: do NOT snapshot positions, stamp the canvas root,
                // or create the drag-selection rect here. d3.drag fires `start`
                // on every mousedown that passes the `filter` — including pure
                // selection clicks that never move past `clickDistance(4)`.
                // Doing visible setup in `start` would flash the dashed
                // bounding rect on every click. Mirrors flow-designer's lazy
                // create-on-first-tick pattern in
                // `draggable-behavior.service.ts:67-138`. The remaining
                // initialization happens in the `drag` handler the first
                // time it fires (which only happens once the cursor has
                // actually moved).
            })
            .on('drag', function (event: d3.D3DragEvent<SVGGElement, TDatum, TDatum>, d: TDatum) {
                if (!d.ui.dragDelta) return;
                const canvasGroup = resolveCanvasRoot();
                // First-tick lazy init. The rect's absence is the
                // source-of-truth for "has this gesture committed to a drag
                // yet?" because (a) it must live anyway for the per-tick
                // visual and (b) mirroring flow-designer's
                // `dragSelection.empty()` check keeps the two pipelines
                // shaped the same way.
                if (canvasGroup.select('rect.drag-selection').empty()) {
                    DragUtils.beginDrag(d, resolveCanvasRoot);
                    // Stamp the canvas root so per-PG `mouseover.drop`
                    // handlers can gate on "is a component drag in progress?"
                    // and exclude self-drops without re-deriving the moving
                    // set themselves.
                    DragUtils.beginCanvasDrag(canvasGroup, d.ui.dragMovingIds ?? new Set<string>());
                    // Append the dashed bounding rect that follows the cursor
                    // in place of per-tick component translation.
                    // `pointer-events: none` keeps it from intercepting
                    // `mouseover.drop` headed for an underlying PG.
                    DragUtils.createDragSelectionRect(canvasGroup, d.ui.dragMovingIds ?? new Set<string>());
                }
                const snapEnabled = !event.sourceEvent.shiftKey;
                DragUtils.updateDragSelectionRect(canvasGroup, event.dx, event.dy, snapEnabled);
            })
            .on('end', function (_event: d3.D3DragEvent<SVGGElement, TDatum, TDatum>, d: TDatum) {
                // Order of operations is deliberate:
                //   1. Read the snap-aligned final delta from the rect, run
                //      `finalizeDrag` (optimistic commit), and invoke the
                //      consumer's `onDragEnd` while the
                //      `g.process-group.drop` affordance is still present so
                //      the canvas can call `DragUtils.readDropTarget`.
                //   2. Remove the rect so a leaked dashed outline can never
                //      persist past a gesture.
                //   3. ALWAYS run `endCanvasDrag` afterwards — including in
                //      the defensive `!d.ui.dragDelta` branch — so a leaked
                //      `is-component-dragging` class can never persist past a
                //      gesture, even if `start` was filtered (in which case
                //      `beginCanvasDrag` never ran and `endCanvasDrag` is a
                //      cheap no-op).
                const canvasGroup = resolveCanvasRoot();
                if (d.ui.dragDelta) {
                    const finalDelta = DragUtils.getDragSelectionDelta(canvasGroup);
                    DragUtils.finalizeDrag(d, finalDelta, resolveCanvasRoot);
                    // Snapshot the canonical moving-set BEFORE deleting it so the
                    // canvas can drive its persistence off the same set the renderer
                    // moved (the consumer's `selectedIds` signal can lag behind in
                    // the unselected-grab case — see `LayerDragEndCallback`).
                    const movingIds = d.ui.dragMovingIds ?? new Set<string>();
                    delete d.ui.dragDelta;
                    delete d.ui.dragMovingIds;
                    // Always emit; the canvas owns the zero-delta short-circuit
                    // so all drag-end side effects (telemetry, HUD, persistence)
                    // share one decision point. See `LayerDragEndCallback`.
                    onDragEnd(finalDelta, movingIds);
                }
                DragUtils.removeDragSelectionRect(canvasGroup);
                DragUtils.endCanvasDrag(canvasGroup);
            });

        newlyMoveable.classed('moveable', true).call(drag);
    }

    /**
     * Snapshot positions for every moving component and bend points for every connection
     * whose bends should translate with the gesture: selected connections plus self-loops
     * attached to a moving component.
     *
     * Also stamps `ui.dragStartEntity` with the pre-gesture entity reference so the
     * canvas's `componentsDragEnd` can publish it as `baselineEntity` on each item.
     * Consumers compute the persisted target from this baseline (position / bends +
     * delta, with `revision` from the same baseline) so a mid-gesture parent push to
     * the same component doesn't redirect the save. See `DragUiState.dragStartEntity`
     * for the contract.
     *
     * @param d the grabbed datum carrying `d.ui.dragMovingIds`
     * @param resolveCanvasRoot resolver for the `g.canvas` selection scoping every query to this canvas
     */
    public static beginDrag(d: { ui?: { dragMovingIds?: Set<string> } }, resolveCanvasRoot: CanvasRootResolver): void {
        const movingIds = d?.ui?.dragMovingIds;
        if (!movingIds || movingIds.size === 0) {
            return;
        }
        const canvasGroup = resolveCanvasRoot();

        canvasGroup.selectAll<SVGGElement, CanvasPositionableDatum>('g.component').each(function (
            datum: CanvasPositionableDatum
        ) {
            if (!datum?.entity?.position || !datum?.ui) {
                return;
            }
            if (!movingIds.has(datum.entity.id)) {
                return;
            }
            datum.ui.dragStartPosition = { ...datum.entity.position };
            datum.ui.currentPosition = { ...datum.entity.position };
            datum.ui.dragStartEntity = datum.entity;
        });

        // Snapshot bends for any connection whose bends should translate with the drag:
        //   1. connections explicitly in `movingIds` (the user selected them)
        //   2. self-loops on a moving component (sourceId === destinationId AND
        //      sourceId is in the moving set) — even if the loop itself is not
        //      selected, the user expects its bends to follow the component.
        canvasGroup.selectAll<SVGGElement, CanvasConnection>('g.connection').each(function (datum: CanvasConnection) {
            const e = datum?.entity;
            if (!e || !Array.isArray(e.bends) || !datum?.ui) {
                return;
            }
            const isMovingConnection = movingIds.has(e.id);
            const isSelfLoopOnMoving = e.sourceId === e.destinationId && movingIds.has(e.sourceId);
            if (!isMovingConnection && !isSelfLoopOnMoving) {
                return;
            }
            datum.ui.dragStartBends = e.bends.map((b: Position) => ({ x: b.x, y: b.y }));
            datum.ui.dragStartEntity = e;
        });
    }

    /**
     * Commit the gesture's snapped `finalDelta` as an optimistic position
     * update on every moving entity:
     *   - components: write `ui.currentPosition = dragStartPosition + delta`
     *     so the renderer's `ui.currentPosition || entity.position` rule
     *     paints the new spot under the disabled-look until the API ack;
     *   - connections: write `ui.bends = dragStartBends + delta` so the
     *     `ConnectionRenderer.calculatePath` cache reflects the new bends.
     * Per-gesture snapshots (`dragStartPosition` / `dragStartBends`) are
     * cleared at the same time. `currentPosition` is intentionally kept
     * past this call — the canvas's success / error handlers clean it up
     * once the API confirms (or rejects) the move.
     *
     * `entity.position` / `entity.bends` are NOT mutated here. The consumer's
     * batched dispatch reads `entity.* + delta` to compute the persisted
     * target, and the gesture-baseline snapshot shares this entity reference
     * — mutating `entity` here would double-apply `delta`.
     *
     * Takes the snap-aligned `finalDelta` as a parameter (rather than
     * deriving it from `d.ui.dragDelta` + `snapEnabled`) so the same source
     * of truth — the drag-selection rect's datum, read via
     * {@link getDragSelectionDelta} — drives both the per-tick visual and
     * the at-drop commit.
     *
     * @param d the grabbed datum carrying `d.ui.dragMovingIds`
     * @param finalDelta the snapped delta read from {@link getDragSelectionDelta}
     * @param resolveCanvasRoot resolver for the `g.canvas` selection scoping every query to this canvas
     */
    public static finalizeDrag(
        d: { ui?: { dragMovingIds?: Set<string> } },
        finalDelta: Position,
        resolveCanvasRoot: CanvasRootResolver
    ): void {
        const movingIds = d?.ui?.dragMovingIds;
        if (!movingIds || movingIds.size === 0) {
            return;
        }
        const canvasGroup = resolveCanvasRoot();

        canvasGroup.selectAll<SVGGElement, CanvasPositionableDatum>('g.component').each(function (
            datum: CanvasPositionableDatum
        ) {
            if (!movingIds.has(datum?.entity?.id)) {
                return;
            }
            if (!datum?.ui?.dragStartPosition) {
                return;
            }
            datum.ui.currentPosition = {
                x: datum.ui.dragStartPosition.x + finalDelta.x,
                y: datum.ui.dragStartPosition.y + finalDelta.y
            };
            delete datum.ui.dragStartPosition;
        });

        canvasGroup.selectAll<SVGGElement, CanvasConnection>('g.connection').each(function (datum: CanvasConnection) {
            if (!datum?.ui?.dragStartBends || !datum?.entity) {
                return;
            }
            const finalBends = (datum.ui.dragStartBends as Position[]).map((b) => ({
                x: b.x + finalDelta.x,
                y: b.y + finalDelta.y
            }));
            datum.ui.bends = finalBends;
            delete datum.ui.dragStartBends;
        });
    }
}
