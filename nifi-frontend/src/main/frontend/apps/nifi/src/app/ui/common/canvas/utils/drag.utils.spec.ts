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
import { DragUtils, DraggableDatum } from './drag.utils';
import { CanvasConstants } from '../canvas.constants';
import { CanvasRootSelection, Dimension } from '../canvas.types';
import { ComponentType, Position } from '@nifi/shared';

const SVG_NS = 'http://www.w3.org/2000/svg';
const SNAP = CanvasConstants.SNAP_ALIGNMENT_PIXELS;

/** Minimal component datum used by DragUtils bbox / move helpers in these tests. */
interface ComponentTestDatum extends DraggableDatum {
    entity: DraggableDatum['entity'] & {
        position: Position;
    };
    ui: DraggableDatum['ui'] & {
        dimensions: Dimension;
        componentType?: ComponentType;
    };
}

/** Minimal connection datum for bend / self-loop drag helpers. */
interface ConnectionTestDatum {
    entity: {
        id: string;
        sourceId: string;
        destinationId: string;
        sourceGroupId?: string;
        destinationGroupId?: string;
        bends: Position[];
    };
    ui: {
        start: Position;
        end: Position;
        bends?: Position[];
        dragDelta?: Position;
        dragMovingIds?: Set<string>;
    };
}

interface DropTargetDatum {
    entity: { id: string };
}

interface DragSelectionRectDatum {
    original: Position;
    x: number;
    y: number;
}

// Each test runs against a fresh `<svg><g class="canvas">…` subtree so injected
// components/connections sit under a real canvas root and DragUtils' scoped
// `selectAll` queries find them. Matches the production DOM where every
// renderer's drag handler walks up to `g.canvas` once at attach-time.
let canvasContainer: SVGGElement;

function canvasGroup(): CanvasRootSelection {
    return d3.select<SVGGElement, unknown>(canvasContainer);
}

interface InjectComponentOptions {
    id: string;
    x: number;
    y: number;
    width?: number;
    height?: number;
    /**
     * The DOM `selected` class is intentionally NOT mirrored from the gesture's
     * moving-set: the production drag handler reads `getSelectedIds()` to compute
     * `dragMovingIds`, never the DOM. We still expose this so the unselected-grab
     * regression can verify DragUtils ignores the DOM class entirely.
     */
    selected?: boolean;
}

interface InjectConnectionOptions {
    id: string;
    sourceId: string;
    destinationId: string;
    sourceGroupId?: string;
    destinationGroupId?: string;
    bends?: Array<{ x: number; y: number }>;
    selected?: boolean;
}

function injectComponent(opts: InjectComponentOptions, parent: Element = canvasContainer): SVGGElement {
    const g = document.createElementNS(SVG_NS, 'g') as SVGGElement;
    g.setAttribute('class', `component${opts.selected ? ' selected' : ''}`);
    const componentDatum: ComponentTestDatum = {
        entity: {
            id: opts.id,
            position: { x: opts.x, y: opts.y },
            permissions: { canRead: true, canWrite: true }
        },
        ui: {
            // `dimensions` is required for the drag-selection rect's bbox
            // computation. Default to the production processor footprint so
            // tests that don't care about exact bbox geometry still get a
            // realistic rect.
            dimensions: {
                width: opts.width ?? CanvasConstants.PROCESSOR.width,
                height: opts.height ?? CanvasConstants.PROCESSOR.height
            }
        }
    };
    d3.select(g).datum(componentDatum);
    parent.appendChild(g);
    return g;
}

interface InjectProcessGroupOptions {
    id: string;
    classes?: string[];
}

/**
 * Inject a `g.process-group` element with a datum mirroring the production
 * shape consumed by `DragUtils.readDropTarget`. The drop helper follows
 * production semantics: only `entity.id` is read, no other fields are
 * inspected.
 */
function injectProcessGroup(opts: InjectProcessGroupOptions, parent: Element = canvasContainer): SVGGElement {
    const g = document.createElementNS(SVG_NS, 'g') as SVGGElement;
    const classes = ['process-group', ...(opts.classes ?? [])].join(' ');
    g.setAttribute('class', classes);
    const dropDatum: DropTargetDatum = { entity: { id: opts.id } };
    d3.select(g).datum(dropDatum);
    parent.appendChild(g);
    return g;
}

function injectConnection(opts: InjectConnectionOptions, parent: Element = canvasContainer): SVGGElement {
    const g = document.createElementNS(SVG_NS, 'g') as SVGGElement;
    g.setAttribute('class', `connection${opts.selected ? ' selected' : ''}`);
    const path = document.createElementNS(SVG_NS, 'path');
    g.appendChild(path);
    const labelContainer = document.createElementNS(SVG_NS, 'g');
    labelContainer.setAttribute('class', 'connection-label-container');
    g.appendChild(labelContainer);
    const connectionDatum: ConnectionTestDatum = {
        entity: {
            id: opts.id,
            sourceId: opts.sourceId,
            destinationId: opts.destinationId,
            sourceGroupId: opts.sourceGroupId,
            destinationGroupId: opts.destinationGroupId,
            bends: opts.bends ? opts.bends.map((b) => ({ ...b })) : []
        },
        ui: { start: { x: 0, y: 0 }, end: { x: 0, y: 0 } }
    };
    d3.select(g).datum(connectionDatum);
    parent.appendChild(g);
    return g;
}

function clearAll(): void {
    document.querySelectorAll('svg').forEach((n) => n.remove());
}

/** Loose read helper — production gesture fields are stamped onto `ui` mid-test. */
interface MutableTestDatum {
    entity: {
        id: string;
        position?: Position;
        permissions?: { canRead: boolean; canWrite: boolean };
        sourceId?: string;
        destinationId?: string;
        sourceGroupId?: string;
        destinationGroupId?: string;
        bends?: Position[];
    };
    ui: {
        dimensions?: Dimension;
        start?: Position;
        end?: Position;
        bends?: Position[];
        dragDelta?: Position;
        dragMovingIds?: Set<string>;
        dragStartPosition?: Position;
        currentPosition?: Position;
        dragStartBends?: Position[];
        dragStartEntity?: unknown;
    };
}

function datum(el: SVGGElement): MutableTestDatum {
    return d3.select(el).datum() as MutableTestDatum;
}

/**
 * Build the per-gesture state the renderer's `start` handler would stash on the
 * grabbed datum: a dragDelta marker (used by attachComponentDrag.end's gate, NOT
 * for accumulation — that lives on the rect's datum now) and the moving-set
 * computed from the canonical `selectedIds` signal.
 */
function withGesture(grabbed: SVGGElement, movingIds: string[]): MutableTestDatum {
    const d = datum(grabbed);
    d.ui.dragMovingIds = new Set(movingIds);
    d.ui.dragDelta = { x: 0, y: 0 };
    return d;
}

/**
 * Read the drag-selection rect from the supplied canvas root, asserting it
 * exists. Tests that need to verify rect absence call
 * `canvasGroup().select('rect.drag-selection').empty()` directly.
 */
function dragRect(group = canvasGroup()): d3.Selection<SVGRectElement, DragSelectionRectDatum, SVGGElement, unknown> {
    const rect = group.select<SVGRectElement>('rect.drag-selection');
    expect(rect.empty()).toBe(false);
    return rect as d3.Selection<SVGRectElement, DragSelectionRectDatum, SVGGElement, unknown>;
}

describe('DragUtils', () => {
    beforeEach(() => {
        const svg = document.createElementNS(SVG_NS, 'svg');
        canvasContainer = document.createElementNS(SVG_NS, 'g') as SVGGElement;
        canvasContainer.setAttribute('class', 'canvas');
        svg.appendChild(canvasContainer);
        document.body.appendChild(svg);
    });

    afterEach(() => {
        clearAll();
        vi.restoreAllMocks();
    });

    describe('beginDrag', () => {
        it('snapshots dragStartPosition / currentPosition for every component in dragMovingIds', () => {
            const c1 = injectComponent({ id: 'c1', x: 100, y: 100 });
            const c2 = injectComponent({ id: 'c2', x: 200, y: 200 });
            // c3 is in the DOM but NOT in dragMovingIds — must not be snapshotted.
            injectComponent({ id: 'c3', x: 300, y: 300 });

            const d = withGesture(c1, ['c1', 'c2']);
            DragUtils.beginDrag(d, canvasGroup);

            expect(datum(c1).ui.dragStartPosition).toEqual({ x: 100, y: 100 });
            expect(datum(c1).ui.currentPosition).toEqual({ x: 100, y: 100 });
            expect(datum(c2).ui.dragStartPosition).toEqual({ x: 200, y: 200 });
        });

        it('snapshots only the grabbed component when dragMovingIds is a singleton', () => {
            const c1 = injectComponent({ id: 'c1', x: 100, y: 100 });
            const c2 = injectComponent({ id: 'c2', x: 200, y: 200 });

            const d = withGesture(c1, ['c1']);
            DragUtils.beginDrag(d, canvasGroup);

            expect(datum(c1).ui.dragStartPosition).toEqual({ x: 100, y: 100 });
            expect(datum(c2).ui.dragStartPosition).toBeUndefined();
        });

        it('snapshots dragStartBends for connections explicitly in dragMovingIds', () => {
            const c1 = injectComponent({ id: 'c1', x: 0, y: 0 });
            const conn = injectConnection({
                id: 'conn-1',
                sourceId: 's',
                destinationId: 'd',
                bends: [
                    { x: 50, y: 60 },
                    { x: 70, y: 80 }
                ]
            });

            const d = withGesture(c1, ['c1', 'conn-1']);
            DragUtils.beginDrag(d, canvasGroup);

            expect(datum(conn).ui.dragStartBends).toEqual([
                { x: 50, y: 60 },
                { x: 70, y: 80 }
            ]);
            // Snapshot is a copy, not the same reference (so translation can't mutate origin)
            expect(datum(conn).ui.dragStartBends).not.toBe(datum(conn).entity.bends);
        });

        it('snapshots dragStartBends for self-loops on a moving component, even when not in dragMovingIds', () => {
            const proc = injectComponent({ id: 'proc-1', x: 0, y: 0 });
            const loop = injectConnection({
                id: 'loop-1',
                sourceId: 'proc-1',
                destinationId: 'proc-1',
                bends: [{ x: 10, y: 20 }]
            });

            const d = withGesture(proc, ['proc-1']);
            DragUtils.beginDrag(d, canvasGroup);

            expect(datum(loop).ui.dragStartBends).toEqual([{ x: 10, y: 20 }]);
        });

        it('does not snapshot bends for unrelated, non-self-loop connections', () => {
            const proc = injectComponent({ id: 'proc-1', x: 0, y: 0 });
            const edge = injectConnection({
                id: 'edge-1',
                sourceId: 'proc-1',
                destinationId: 'proc-2',
                bends: [{ x: 10, y: 20 }]
            });

            const d = withGesture(proc, ['proc-1']);
            DragUtils.beginDrag(d, canvasGroup);

            expect(datum(edge).ui.dragStartBends).toBeUndefined();
        });
    });

    // The dashed bounding rect is the only mid-gesture visual under the new
    // model. It replaces per-tick component / connection translation entirely
    // and (because of `pointer-events: none`) never intercepts mouseover
    // headed for an underlying PG. These tests cover the rect's lifecycle,
    // bbox computation, snap-aligned tick updates, and final-delta read.
    describe('drag-selection rect', () => {
        describe('createDragSelectionRect', () => {
            it('appends a single rect.drag-selection under the supplied canvas root', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));

                const rects = canvasContainer.querySelectorAll('rect.drag-selection');
                expect(rects.length).toBe(1);
                const rect = rects[0] as SVGRectElement;
                expect(rect.parentNode).toBe(canvasContainer);
            });

            it('sets pointer-events="none" on the rect so it never intercepts mouseover.drop', () => {
                injectComponent({ id: 'c1', x: 0, y: 0, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));

                const rect = canvasContainer.querySelector('rect.drag-selection') as SVGRectElement;
                expect(rect.getAttribute('pointer-events')).toBe('none');
            });

            it('sizes the rect to the bounding box of every moving component (entity.position + ui.dimensions) plus padding', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                injectComponent({ id: 'c2', x: 200, y: 200, width: 80, height: 40 });
                injectComponent({ id: 'unrelated', x: 0, y: 0, width: 99, height: 99 });

                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1', 'c2']));

                const rect = dragRect();
                // bbox of (100,100..150,130) ∪ (200,200..280,240) = (100,100..280,240)
                // padding=4 expands to (96,96..284,244): w=188, h=148
                expect(parseFloat(rect.attr('x'))).toBe(96);
                expect(parseFloat(rect.attr('y'))).toBe(96);
                expect(parseFloat(rect.attr('width'))).toBe(188);
                expect(parseFloat(rect.attr('height'))).toBe(148);
            });

            it('includes connection bends in the bbox when the connection is in the moving-set', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                injectConnection({
                    id: 'conn-1',
                    sourceId: 'sX',
                    destinationId: 'dY',
                    bends: [
                        { x: 10, y: 10 },
                        { x: 400, y: 400 }
                    ]
                });

                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1', 'conn-1']));

                const rect = dragRect();
                // Component bbox alone: (100,100..150,130). Bends extend min to (10,10)
                // and max to (400,400). With padding=4: (6,6..404,404).
                expect(parseFloat(rect.attr('x'))).toBe(6);
                expect(parseFloat(rect.attr('y'))).toBe(6);
                expect(parseFloat(rect.attr('width'))).toBe(398);
                expect(parseFloat(rect.attr('height'))).toBe(398);
            });

            it('includes self-loop bends in the bbox when the loop is on a moving component (loop not in moving-set)', () => {
                injectComponent({ id: 'proc-1', x: 100, y: 100, width: 50, height: 30 });
                injectConnection({
                    id: 'loop-1',
                    sourceId: 'proc-1',
                    destinationId: 'proc-1',
                    bends: [{ x: 200, y: 200 }]
                });

                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['proc-1']));

                const rect = dragRect();
                // Component bbox (100,100..150,130) ∪ self-loop bend (200,200)
                // = (100,100..200,200). Padding=4 → (96,96..204,204).
                expect(parseFloat(rect.attr('x'))).toBe(96);
                expect(parseFloat(rect.attr('y'))).toBe(96);
                expect(parseFloat(rect.attr('width'))).toBe(108);
                expect(parseFloat(rect.attr('height'))).toBe(108);
            });

            it('binds {original, x, y} to the rect so per-tick updates and drag-end can read the running delta', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));

                const rectDatum = dragRect().datum() as { original: { x: number; y: number }; x: number; y: number };
                expect(rectDatum.original).toEqual({ x: 96, y: 96 });
                expect(rectDatum.x).toBe(96);
                expect(rectDatum.y).toBe(96);
            });

            it('is a no-op when movingIds is empty', () => {
                injectComponent({ id: 'c1', x: 0, y: 0 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set<string>());

                expect(canvasContainer.querySelector('rect.drag-selection')).toBeNull();
            });

            it('is a no-op when the moving-set yields no usable rectangle / bend', () => {
                // c1 is in the moving-set but its DOM element doesn't carry an
                // entity / dimensions — the contributor loop guards this and
                // contributes nothing, leaving the bbox unset.
                const g = document.createElementNS(SVG_NS, 'g') as SVGGElement;
                g.setAttribute('class', 'component');
                const incompleteDatum: MutableTestDatum = { entity: { id: 'c1' }, ui: {} };
                d3.select(g).datum(incompleteDatum);
                canvasContainer.appendChild(g);

                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));

                expect(canvasContainer.querySelector('rect.drag-selection')).toBeNull();
            });
        });

        describe('updateDragSelectionRect', () => {
            beforeEach(() => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));
            });

            it('accumulates dx/dy into the rect datum (unsnapped) so successive ticks compose without rounding drift', () => {
                DragUtils.updateDragSelectionRect(canvasGroup(), 3, 5, false);
                DragUtils.updateDragSelectionRect(canvasGroup(), 4, -1, false);

                const d = dragRect().datum() as { x: number; y: number };
                // Original 96 + 3 + 4 = 103; 96 + 5 - 1 = 100.
                expect(d.x).toBe(103);
                expect(d.y).toBe(100);
            });

            it('writes the unsnapped position to x/y attrs when snapEnabled=false', () => {
                DragUtils.updateDragSelectionRect(canvasGroup(), 3, 5, false);

                const rect = dragRect();
                expect(parseFloat(rect.attr('x'))).toBe(99);
                expect(parseFloat(rect.attr('y'))).toBe(101);
            });

            it('snaps x/y to the alignment grid when snapEnabled=true while the datum stays at the unsnapped position', () => {
                DragUtils.updateDragSelectionRect(canvasGroup(), 3, 3, true);

                const rect = dragRect();
                // datum: 96+3=99, 96+3=99; snapped attr: round(99/SNAP)*SNAP each side.
                expect(parseFloat(rect.attr('x'))).toBe(Math.round(99 / SNAP) * SNAP);
                expect(parseFloat(rect.attr('y'))).toBe(Math.round(99 / SNAP) * SNAP);
                const d = rect.datum() as { x: number; y: number };
                expect(d.x).toBe(99);
                expect(d.y).toBe(99);
            });

            it('honors the user toggling shift mid-gesture by reading snapEnabled at write time', () => {
                DragUtils.updateDragSelectionRect(canvasGroup(), 3, 0, false); // shift held → unsnapped
                expect(parseFloat(dragRect().attr('x'))).toBe(99);
                DragUtils.updateDragSelectionRect(canvasGroup(), 0, 0, true); // shift released → snapped
                expect(parseFloat(dragRect().attr('x'))).toBe(Math.round(99 / SNAP) * SNAP);
            });

            it('is a no-op when no rect exists (filtered start handler)', () => {
                DragUtils.removeDragSelectionRect(canvasGroup());
                expect(() => DragUtils.updateDragSelectionRect(canvasGroup(), 5, 5, true)).not.toThrow();
                expect(canvasContainer.querySelector('rect.drag-selection')).toBeNull();
            });
        });

        describe('removeDragSelectionRect', () => {
            it('removes the rect from the canvas root', () => {
                injectComponent({ id: 'c1', x: 0, y: 0 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));
                expect(canvasContainer.querySelector('rect.drag-selection')).not.toBeNull();

                DragUtils.removeDragSelectionRect(canvasGroup());

                expect(canvasContainer.querySelector('rect.drag-selection')).toBeNull();
            });

            it('is idempotent when no rect exists', () => {
                expect(() => DragUtils.removeDragSelectionRect(canvasGroup())).not.toThrow();
                expect(canvasContainer.querySelector('rect.drag-selection')).toBeNull();
            });
        });

        describe('getDragSelectionDelta', () => {
            it('returns {0,0} when no rect exists', () => {
                expect(DragUtils.getDragSelectionDelta(canvasGroup())).toEqual({ x: 0, y: 0 });
            });

            it('returns the snap-aligned delta from the rects displayed x/y vs original', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));
                DragUtils.updateDragSelectionRect(canvasGroup(), 9, 17, true);

                const expectedX = Math.round((96 + 9) / SNAP) * SNAP - 96;
                const expectedY = Math.round((96 + 17) / SNAP) * SNAP - 96;
                expect(DragUtils.getDragSelectionDelta(canvasGroup())).toEqual({ x: expectedX, y: expectedY });
            });

            it('returns the raw delta when the gesture ran with snap disabled', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));
                DragUtils.updateDragSelectionRect(canvasGroup(), 7, -3, false);

                expect(DragUtils.getDragSelectionDelta(canvasGroup())).toEqual({ x: 7, y: -3 });
            });

            it('returns {0,0} when the gesture ran but never moved (drop-in-place)', () => {
                injectComponent({ id: 'c1', x: 100, y: 100, width: 50, height: 30 });
                DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c1']));

                expect(DragUtils.getDragSelectionDelta(canvasGroup())).toEqual({ x: 0, y: 0 });
            });
        });
    });

    describe('finalizeDrag', () => {
        it('commits the supplied finalDelta to every moving component as ui.currentPosition and clears dragStartPosition', () => {
            const c1 = injectComponent({ id: 'c1', x: 100, y: 100 });
            const d = withGesture(c1, ['c1']);
            DragUtils.beginDrag(d, canvasGroup);

            DragUtils.finalizeDrag(d, { x: 16, y: -8 }, canvasGroup);

            expect(datum(c1).ui.currentPosition).toEqual({ x: 116, y: 92 });
            expect(datum(c1).ui.dragStartPosition).toBeUndefined();
        });

        // CRITICAL: finalizeDrag must NOT mutate entity.bends. The consumer's batched
        // dispatch reads `connection.bends + delta` to compute the persisted target, and
        // the snapshot shares this entity reference. Mutating entity.bends here would
        // double-apply `delta`.
        it('commits the supplied finalDelta to ui.bends ONLY for snapshotted connections, leaving entity.bends at pre-drag, and clears dragStartBends', () => {
            const c = injectComponent({ id: 'c1', x: 0, y: 0 });
            const conn = injectConnection({
                id: 'conn-1',
                sourceId: 's',
                destinationId: 'd',
                bends: [{ x: 50, y: 60 }]
            });
            const d = withGesture(c, ['c1', 'conn-1']);
            DragUtils.beginDrag(d, canvasGroup);

            DragUtils.finalizeDrag(d, { x: 8, y: 8 }, canvasGroup);

            expect(datum(conn).ui.bends).toEqual([{ x: 58, y: 68 }]);
            expect(datum(conn).entity.bends).toEqual([{ x: 50, y: 60 }]);
            expect(datum(conn).ui.dragStartBends).toBeUndefined();
        });

        it('commits self-loop bends to ui ONLY (entity stays pre-drag) and clears the snapshot even when the loop was never in dragMovingIds', () => {
            const proc = injectComponent({ id: 'proc-1', x: 0, y: 0 });
            const loop = injectConnection({
                id: 'loop-1',
                sourceId: 'proc-1',
                destinationId: 'proc-1',
                bends: [{ x: 10, y: 20 }]
            });
            const d = withGesture(proc, ['proc-1']);
            DragUtils.beginDrag(d, canvasGroup);

            DragUtils.finalizeDrag(d, { x: 4, y: -2 }, canvasGroup);

            expect(datum(loop).ui.bends).toEqual([{ x: 14, y: 18 }]);
            expect(datum(loop).entity.bends).toEqual([{ x: 10, y: 20 }]);
            expect(datum(loop).ui.dragStartBends).toBeUndefined();
        });

        it('is a no-op when dragMovingIds is empty', () => {
            const c = injectComponent({ id: 'c1', x: 100, y: 100 });
            const d = datum(c);
            d.ui.dragMovingIds = new Set<string>();

            expect(() => DragUtils.finalizeDrag(d, { x: 8, y: 8 }, canvasGroup)).not.toThrow();
            expect(datum(c).ui.currentPosition).toBeUndefined();
        });
    });

    describe('dedup: self-loop also explicitly in dragMovingIds', () => {
        it('snapshots once (no double-translation) when the loop is BOTH in dragMovingIds AND a self-loop', () => {
            const proc = injectComponent({ id: 'proc-1', x: 0, y: 0 });
            const loop = injectConnection({
                id: 'loop-1',
                sourceId: 'proc-1',
                destinationId: 'proc-1',
                bends: [{ x: 10, y: 20 }]
            });
            // The loop is BOTH in dragMovingIds AND a self-loop on a moving component.
            const d = withGesture(proc, ['proc-1', 'loop-1']);
            DragUtils.beginDrag(d, canvasGroup);

            // dragStartBends was captured exactly once — if the snapshot were
            // taken twice the second pass would translate from already-stamped
            // bends and the final committed value would be off by one delta.
            DragUtils.finalizeDrag(d, { x: 10, y: 10 }, canvasGroup);

            // ui.bends carries the final optimistic value; entity.bends stays pre-drag
            // (consumer adds delta exactly once).
            expect(datum(loop).ui.bends).toEqual([{ x: 20, y: 30 }]);
            expect(datum(loop).entity.bends).toEqual([{ x: 10, y: 20 }]);
        });
    });

    describe('regression: unselected-grab while a multi-selection exists', () => {
        // Reproduces the bug class where DragUtils trusted the DOM `selected` class:
        // grabbing an unselected component while A and B are still DOM-selected would
        // incorrectly drag A and B along. With the new contract, DragUtils reads only
        // `d.ui.dragMovingIds` (computed from the canonical signal at start time), so
        // a singleton moving-set drags only the grabbed component.
        it('commits only the grabbed component, never the abandoned DOM-selected ones', () => {
            const a = injectComponent({ id: 'a', x: 100, y: 100, selected: true });
            const b = injectComponent({ id: 'b', x: 200, y: 200, selected: true });
            // The user grabs c, which is NOT in the prior multi-selection. The
            // renderer's start handler computes movingIds from the canonical signal
            // (now { c }), even though A and B still carry the DOM `selected` class.
            const c = injectComponent({ id: 'c', x: 300, y: 300, selected: false });

            const d = withGesture(c, ['c']);
            DragUtils.beginDrag(d, canvasGroup);
            DragUtils.finalizeDrag(d, { x: 10, y: 10 }, canvasGroup);

            // Only c was snapshotted and committed.
            expect(datum(c).ui.currentPosition).toEqual({ x: 310, y: 310 });
            // A and B were never touched even though they had the DOM `selected` class.
            expect(datum(a).ui.dragStartPosition).toBeUndefined();
            expect(datum(a).ui.currentPosition).toBeUndefined();
            expect(datum(b).ui.dragStartPosition).toBeUndefined();
            expect(datum(b).ui.currentPosition).toBeUndefined();
        });

        it('drags only the grabbed component in the rect, never the abandoned DOM-selected ones', () => {
            injectComponent({ id: 'a', x: 100, y: 100, width: 50, height: 30, selected: true });
            injectComponent({ id: 'b', x: 200, y: 200, width: 50, height: 30, selected: true });
            const c = injectComponent({ id: 'c', x: 300, y: 300, width: 50, height: 30, selected: false });

            // Only c is in the moving-set. The rect must size to c's bbox alone.
            DragUtils.createDragSelectionRect(canvasGroup(), new Set(['c']));

            const rect = dragRect();
            // c spans (300,300..350,330). Padding=4 → (296,296..354,334).
            expect(parseFloat(rect.attr('x'))).toBe(296);
            expect(parseFloat(rect.attr('y'))).toBe(296);
            expect(parseFloat(rect.attr('width'))).toBe(58);
            expect(parseFloat(rect.attr('height'))).toBe(38);
            // Reference c so the assertion arrays don't strand unused.
            expect(c).toBeTruthy();
        });
    });

    // Ensures DragUtils' selectAll queries are scoped to the supplied canvasGroup
    // and not the document. Two canvases co-existing on the same page would
    // otherwise cross-talk: a gesture on canvas A would mutate canvas B's
    // component / connection ui state, or stamp canvas B's DOM with the
    // drag-selection rect.
    describe('multi-canvas isolation', () => {
        it('only snapshots / finalizes components and connections inside the supplied canvasGroup, and only stamps the rect on that canvas', () => {
            // Build a SECOND canvas alongside the default one created in beforeEach.
            const otherSvg = document.createElementNS(SVG_NS, 'svg');
            const otherCanvas = document.createElementNS(SVG_NS, 'g') as SVGGElement;
            otherCanvas.setAttribute('class', 'canvas');
            otherSvg.appendChild(otherCanvas);
            document.body.appendChild(otherSvg);

            // Canvas A: the gesture happens here.
            const aProc = injectComponent({ id: 'a-proc', x: 100, y: 100, width: 50, height: 30 });
            const aLoop = injectConnection({
                id: 'a-loop',
                sourceId: 'a-proc',
                destinationId: 'a-proc',
                bends: [{ x: 110, y: 90 }]
            });

            // Canvas B: must remain UNTOUCHED. Same IDs as A to make accidental
            // global selectAll queries trip the assertion (any moving-set membership
            // would match these too).
            const bProc = injectComponent({ id: 'a-proc', x: 500, y: 500 }, otherCanvas);
            const bLoop = injectConnection(
                {
                    id: 'a-loop',
                    sourceId: 'a-proc',
                    destinationId: 'a-proc',
                    bends: [{ x: 510, y: 490 }]
                },
                otherCanvas
            );

            const d = withGesture(aProc, ['a-proc']);
            DragUtils.beginDrag(d, canvasGroup);
            DragUtils.createDragSelectionRect(canvasGroup(), new Set(['a-proc']));
            DragUtils.updateDragSelectionRect(canvasGroup(), 7, -3, false);
            const finalDelta = DragUtils.getDragSelectionDelta(canvasGroup());
            DragUtils.finalizeDrag(d, finalDelta, canvasGroup);
            DragUtils.removeDragSelectionRect(canvasGroup());

            // Canvas A was snapshotted and committed.
            expect(datum(aProc).ui.currentPosition).toEqual({ x: 107, y: 97 });
            expect(datum(aLoop).ui.bends).toEqual([{ x: 117, y: 87 }]);

            // Canvas B was completely ignored.
            expect(datum(bProc).ui.dragStartPosition).toBeUndefined();
            expect(datum(bProc).ui.currentPosition).toBeUndefined();
            expect(datum(bLoop).ui.dragStartBends).toBeUndefined();
            expect(datum(bLoop).ui.bends).toBeUndefined();
            expect(datum(bLoop).entity.bends).toEqual([{ x: 510, y: 490 }]);
            // No drag-selection rect stamped on canvas B at any point.
            expect(otherCanvas.querySelector('rect.drag-selection')).toBeNull();
        });
    });

    // Phase 0d: canvas-root drag stamps + drop-target helper. The PG renderer's
    // `mouseover.drop` reads the active-drag class and the moving-set property
    // off the canvas root; the canvas component's `onDragEnd` reads the unique
    // `g.process-group.drop` element's datum to project `targetGroupId` onto
    // the `componentsDragEnd` payload. Each test exercises one helper in
    // isolation so a regression in begin/end/read does not mask the others.
    describe('canvas-root drag stamps', () => {
        it('beginCanvasDrag sets is-component-dragging and stamps dragMovingIds when movingIds is non-empty', () => {
            const movingIds = new Set(['a', 'b']);
            DragUtils.beginCanvasDrag(canvasGroup(), movingIds);

            expect(canvasContainer.classList.contains(DragUtils.DRAGGING_CLASS)).toBe(true);
            // Structural equality, not reference equality: the helper stamps a
            // defensive snapshot so the caller cannot mutate the live stamp.
            expect(DragUtils.getActiveDragMovingIds(canvasGroup())).toEqual(movingIds);
            expect(DragUtils.getActiveDragMovingIds(canvasGroup())).not.toBe(movingIds);
        });

        it('beginCanvasDrag snapshots movingIds so mutating the input after the call does not change what getActiveDragMovingIds returns', () => {
            const movingIds = new Set(['a', 'b']);
            DragUtils.beginCanvasDrag(canvasGroup(), movingIds);

            movingIds.add('c');
            movingIds.delete('a');

            const stamped = DragUtils.getActiveDragMovingIds(canvasGroup());
            expect(stamped).not.toBeNull();
            expect(stamped!.has('a')).toBe(true);
            expect(stamped!.has('b')).toBe(true);
            expect(stamped!.has('c')).toBe(false);
        });

        it('beginCanvasDrag is a no-op when movingIds is empty (no class, no property stamp)', () => {
            DragUtils.beginCanvasDrag(canvasGroup(), new Set<string>());

            expect(canvasContainer.classList.contains(DragUtils.DRAGGING_CLASS)).toBe(false);
            expect(DragUtils.getActiveDragMovingIds(canvasGroup())).toBeNull();
        });

        it('endCanvasDrag clears the class, the dragMovingIds property, and any lingering .drop on PGs', () => {
            DragUtils.beginCanvasDrag(canvasGroup(), new Set(['a']));
            const lingering = injectProcessGroup({ id: 'pg1', classes: [DragUtils.DROP_CLASS] });
            const otherDrop = injectProcessGroup({ id: 'pg2', classes: [DragUtils.DROP_CLASS] });
            // Permission gate would normally prevent multiple .drop entries,
            // but endCanvasDrag is the safety net for any leak — sweep all of
            // them.

            DragUtils.endCanvasDrag(canvasGroup());

            expect(canvasContainer.classList.contains(DragUtils.DRAGGING_CLASS)).toBe(false);
            expect(DragUtils.getActiveDragMovingIds(canvasGroup())).toBeNull();
            expect(lingering.classList.contains(DragUtils.DROP_CLASS)).toBe(false);
            expect(otherDrop.classList.contains(DragUtils.DROP_CLASS)).toBe(false);
        });

        it('endCanvasDrag is idempotent — calling without a prior beginCanvasDrag is a safe no-op', () => {
            // No begin call. The mouseout-on-empty-canvas case must be safe.
            expect(() => DragUtils.endCanvasDrag(canvasGroup())).not.toThrow();
            expect(canvasContainer.classList.contains(DragUtils.DRAGGING_CLASS)).toBe(false);
            expect(DragUtils.getActiveDragMovingIds(canvasGroup())).toBeNull();
        });
    });

    describe('readDropTarget', () => {
        it('returns the entity id of the unique g.process-group.drop element', () => {
            injectProcessGroup({ id: 'pg-other' });
            injectProcessGroup({ id: 'pg-target', classes: [DragUtils.DROP_CLASS] });

            expect(DragUtils.readDropTarget(canvasGroup())).toBe('pg-target');
        });

        it('returns null when no PG carries the .drop class (drop on empty canvas)', () => {
            injectProcessGroup({ id: 'pg1' });
            injectProcessGroup({ id: 'pg2' });

            expect(DragUtils.readDropTarget(canvasGroup())).toBeNull();
        });

        it('returns null when the canvas has no process groups at all', () => {
            expect(DragUtils.readDropTarget(canvasGroup())).toBeNull();
        });
    });
});
