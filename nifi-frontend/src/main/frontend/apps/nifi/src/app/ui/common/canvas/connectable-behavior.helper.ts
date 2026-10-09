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
import { CanvasComponentRef, CanvasConnectableEntity } from '../../../state/flow-shared';
import {
    CanvasFunnel,
    CanvasPort,
    CanvasProcessGroup,
    CanvasProcessor,
    CanvasRemoteProcessGroup,
    CanvasRootResolver
} from './canvas.types';

// Local references to canvas components flow through the shared
// `CanvasComponentRef` (state/flow-shared) so the drag pipeline
// (`componentsDragEnd.items`, `markItemsSaving`) and the connection-handle
// interaction agree on one `{ id, type }` shape.

/**
 * Canvas datum kinds that can participate in the connection-handle
 * interaction. Labels and connections are intentionally excluded because
 * neither is a valid connection source or destination.
 *
 * Retaining the concrete wrapper union guarantees both the positioned entity
 * fields needed for geometry and the full canonical entity passed through
 * create/reconnect gesture payloads.
 */
export type ConnectableComponentDatum =
    | CanvasProcessor
    | CanvasFunnel
    | CanvasPort
    | CanvasProcessGroup
    | CanvasRemoteProcessGroup;

/**
 * Resolve the position currently rendered for a connectable component.
 * Optimistic position state is frontend-only; the canonical entity remains
 * unchanged until a server response supplies the persisted position.
 */
export function resolveConnectablePosition(datum: ConnectableComponentDatum): Position {
    return datum.ui.currentPosition ?? datum.entity.position;
}

/**
 * d3.Selection alias for a `g.component` that participates in the
 * connect-handle drag (one of the connectable canvas wrappers above).
 */
export type ConnectableComponentSelection = d3.Selection<SVGGElement, ConnectableComponentDatum, d3.BaseType, unknown>;

/**
 * Datum bound to the `text.add-connect` glyph appended to the source
 * component on hover. Captures the glyph's resting position so a no-op
 * drag (drop outside any destination) can snap the glyph back.
 */
interface AddConnectDatum {
    origX: number;
    origY: number;
}

/**
 * Subject returned by the d3.drag `.subject()` accessor for the
 * connect-handle drag. Carries the canvas-space drag origin and a
 * back-reference to the SVGTextElement so the start handler can re-parent
 * the glyph onto the canvas root for free movement.
 */
interface AddConnectSubject {
    x: number;
    y: number;
    element: SVGTextElement;
    origin: [number, number];
}

/**
 * Datum bound to the in-flight `path.connector` preview. Captures the
 * source's id and the geometry needed to render the snap-to-edge endpoint
 * without re-querying the source's selection on every drag tick.
 */
interface ConnectorPathDatum {
    sourceId: string;
    sourceWidth: number;
    x: number;
    y: number;
}

/**
 * Concrete d3 drag event type used by every handler in the connect-handle
 * pipeline. Mirrors the typed-event pattern `DragUtils.attachComponentDrag`
 * uses on the component drag.
 */
type AddConnectDragEvent = d3.D3DragEvent<SVGTextElement, AddConnectDatum, AddConnectSubject>;

/**
 * Payload emitted when the user successfully drags from a source's connection
 * handle onto a valid destination component. The host canvas converts this
 * into either an NgRx dispatch (flow-designer) or an output emission
 * (`createConnectionRequested`).
 *
 * The shape mirrors what the legacy `flow-designer` `CreateConnectionRequest`
 * needs without depending on flow-designer-specific types — pages can pass
 * `entity` straight through.
 */
export interface CreateConnectionPayload {
    source: CanvasComponentRef & { entity: CanvasConnectableEntity };
    destination: CanvasComponentRef & { entity: CanvasConnectableEntity };
    bends?: Position[];
}

/**
 * Visual scaffolding for the in-flight connector preview. Every member is
 * universal math on rectangular components and ships a sensible default in
 * `DEFAULT_GEOMETRY`; hosts only need to override entries here when their
 * canvas geometry differs (e.g. a non-default component width that throws
 * the self-loop offsets, or a real bend-collision-avoidance algorithm).
 *
 * Kept separate from `ConnectablePolicy` because policy decides whether the
 * connection handle ever appears, and only when policy permits does any of
 * this geometry run.
 */
export interface ConnectableGeometry {
    /**
     * Computes the point on the perimeter of the bounding box that intersects
     * the line from the perimeter centre to the supplied point. Used to draw
     * the in-flight connector preview that "snaps" to the destination's edge.
     */
    getPerimeterPoint(p: Position, bBox: { x: number; y: number; width: number; height: number }): Position;

    /**
     * Calculates initial bend points for a brand-new connection between the
     * given source and destination data. Used to avoid drawing on top of any
     * existing connections between the same pair of components, and to render
     * a sensible self-loop when source === destination.
     */
    calculateInitialBendPoints(
        sourceData: ConnectableComponentDatum,
        destinationData: ConnectableComponentDatum
    ): Position[];

    /**
     * Self-loop X offset used when drawing the in-flight connector preview
     * for a self-referencing drop. Tuned to keep the preview clear of the
     * source component visually; the default matches the `canvas-test`
     * sandbox's processor width. Mirrors `ConnectionManager.SELF_LOOP_X_OFFSET`
     * in the flow-designer canvas.
     */
    selfLoopXOffset: number;

    /**
     * Self-loop Y offset used when drawing the in-flight connector preview
     * for a self-referencing drop. Mirrors `ConnectionManager.SELF_LOOP_Y_OFFSET`
     * in the flow-designer canvas.
     */
    selfLoopYOffset: number;
}

const TWO_PI: number = 2 * Math.PI;

/**
 * Default `ConnectableGeometry` baked into `ConnectableBehaviorHelper`.
 * Hosts opt into overrides via `ConnectablePolicy.geometry`; without an
 * override, every method here is what runs.
 *
 * - `getPerimeterPoint` — universal trig on a rectangular bbox; matches the
 *   port from flow-designer's `CanvasUtils.getPerimeterPoint`.
 * - `calculateInitialBendPoints` — sensible no-op (`[]`); the host can plug
 *   in collision-avoidance math if its canvas needs it (flow-designer does).
 * - `selfLoopX/YOffset` — chosen to clear a 350-wide processor (the
 *   `canvas-test` default), with the hand-tuned 25px vertical offset.
 */
export const DEFAULT_GEOMETRY: ConnectableGeometry = {
    getPerimeterPoint(p: Position, bBox: { x: number; y: number; width: number; height: number }): Position {
        const theta: number = Math.atan2(bBox.height, bBox.width);

        const xRadius: number = bBox.width / 2;
        const yRadius: number = bBox.height / 2;

        const cx: number = bBox.x + xRadius;
        const cy: number = bBox.y + yRadius;

        const dx: number = p.x - cx;
        const dy: number = p.y - cy;
        let alpha: number = Math.atan2(dy, dx);

        alpha = alpha % TWO_PI;
        if (alpha < 0) {
            alpha += TWO_PI;
        }

        const beta: number = Math.PI / 2 - alpha;

        if ((alpha >= 0 && alpha < theta) || (alpha >= TWO_PI - theta && alpha < TWO_PI)) {
            return {
                x: bBox.x + bBox.width,
                y: cy + Math.tan(alpha) * xRadius
            };
        } else if (alpha >= theta && alpha < Math.PI - theta) {
            return {
                x: cx + Math.tan(beta) * yRadius,
                y: bBox.y + bBox.height
            };
        } else if (alpha >= Math.PI - theta && alpha < Math.PI + theta) {
            return {
                x: bBox.x,
                y: cy - Math.tan(alpha) * xRadius
            };
        } else {
            return {
                x: cx - Math.tan(beta) * yRadius,
                y: bBox.y
            };
        }
    },
    calculateInitialBendPoints(
        _sourceData: ConnectableComponentDatum,
        _destinationData: ConnectableComponentDatum
    ): Position[] {
        return [];
    },
    selfLoopXOffset: 350 / 2 + 5,
    selfLoopYOffset: 25
};

/**
 * Policy adapter the host canvas supplies to `ConnectableBehaviorHelper`.
 *
 * Two responsibilities:
 *
 * 1. **Permission policy (required)** — `isValidConnectionSource` /
 *    `isValidConnectionDestination` decide whether the connection handle
 *    ever appears on hover and whether a drop target is accepted on
 *    release. These run at fire-time and are the only methods every host
 *    must implement.
 * 2. **Geometry overrides (optional)** — `geometry` lets a host plug in a
 *    partial `ConnectableGeometry` (e.g. flow-designer overriding
 *    `calculateInitialBendPoints` with its real collision-avoidance
 *    algorithm). Anything not overridden falls through to `DEFAULT_GEOMETRY`.
 *
 * The dependency points policy → geometry; nothing in geometry runs until
 * policy permits the action, which is why the two live behind one type
 * with a single optional escape hatch rather than two separate inputs.
 */
export interface ConnectablePolicy {
    /**
     * Whether the d3 selection represents a component that can act as the
     * source of a new connection (e.g. processor with read+modify, RPG, input
     * port, funnel, process group).
     */
    isValidConnectionSource(selection: ConnectableComponentSelection): boolean;

    /**
     * Whether the d3 selection represents a component that can accept a new
     * connection as its destination (e.g. processor with modify and an input
     * requirement that is not `INPUT_FORBIDDEN`, RPG, output port, funnel,
     * process group).
     */
    isValidConnectionDestination(selection: ConnectableComponentSelection): boolean;

    /**
     * Optional partial geometry overrides. Anything left undefined falls
     * through to `DEFAULT_GEOMETRY`. Most hosts can omit this entirely;
     * only override when a real visual difference (different component
     * width breaking the self-loop offsets, real bend-collision avoidance,
     * etc.) demands it.
     */
    geometry?: Partial<ConnectableGeometry>;
}

/**
 * Callbacks the host canvas supplies to the helper. These replace the
 * direct NgRx dispatches the legacy `ConnectableBehavior` service used.
 */
export interface ConnectableCallbacks {
    /**
     * Invoked when the user begins dragging from a source's connection handle
     * to indicate that the source should become the sole selected component.
     */
    onSelectSource: (source: CanvasComponentRef) => void;

    /**
     * Invoked when the user drops on a valid connection destination. The host
     * canvas owns the side-effects (open the create-connection dialog,
     * dispatch state, emit an output, etc.).
     */
    onConnectionRequested: (payload: CreateConnectionPayload) => void;

    /**
     * Optional fire-time predicate consulted on `mouseenter.connectable` so
     * components that are disabled (e.g. mid-save: `markItemsSaving` set the
     * id in one of the canvas's `savingXxx` signals) cannot become a
     * connection source. Without this gate, a saving component still surfaces
     * the connection handle and the user can start drawing a connection from
     * it — which then races with the in-flight position save and breaks the
     * "first to save wins" optimistic-locking invariant the drag pipeline
     * relies on. Read at fire-time so toggles take effect without re-attaching
     * the helper.
     *
     * Mirrors the per-id `getDisabledIds()` filter the drag pipeline uses
     * (`AttachComponentDragOptions.getDisabledIds`) so both interactions
     * agree on "this component is currently locked".
     */
    isDisabled?: (componentId: string) => boolean;
}

/**
 * Headless port of the flow-designer `ConnectableBehavior` service. Owns the
 * in-flight d3 drag interaction for drawing a new connection between
 * components without depending on NgRx, `CanvasUtils`, or
 * `ConnectionManager`.
 *
 * Wire-up:
 * 1. Construct with a `ConnectablePolicy` adapter + callbacks.
 * 2. Call `activate(componentSelection)` when `canEdit` is true to attach the
 *    hover/drag listeners.
 * 3. Call `deactivate(componentSelection)` when `canEdit` flips to false.
 */
export class ConnectableBehaviorHelper {
    private readonly connect: d3.DragBehavior<SVGTextElement, AddConnectDatum, AddConnectSubject>;
    private origin: [number, number] | null = null;
    private readonly geometry: ConnectableGeometry;

    constructor(
        private readonly policy: ConnectablePolicy,
        private readonly callbacks: ConnectableCallbacks,
        private readonly resolveCanvasRoot: CanvasRootResolver
    ) {
        // Merge the host's optional geometry overrides over the universal
        // defaults exactly once, at construction. Per-gesture lookups read a
        // single resolved object so the hot path is the same shape every
        // time, no matter which fields the host overrode (or didn't).
        this.geometry = { ...DEFAULT_GEOMETRY, ...policy.geometry };
        this.connect = this.buildDragBehavior();
    }

    /**
     * Activate the connection handle on each component in the selection.
     * Adds hover handlers that surface the `text.add-connect` glyph and wire
     * the drag behavior. Safe to call repeatedly — the legacy implementation
     * relies on the `text.add-connect` element being created once per hover.
     */
    public activate<TDatum extends ConnectableComponentDatum>(
        components: d3.Selection<SVGGElement, TDatum, d3.BaseType, unknown>
    ): void {
        components
            .classed('connectable', true)
            .on('mouseenter.connectable', (event: MouseEvent, d: TDatum) => {
                if (!this.allowConnection(event)) {
                    return;
                }
                // Saving-state gate. Mirrors `DragUtils.attachComponentDrag`'s
                // `getDisabledIds()` filter so a component locked by an in-flight
                // save (canvas's `savingXxx` signals → `disabledXxxIds()`) cannot
                // also become a connection source mid-save. Reading the callback
                // at fire-time means a toggle of the saving set takes effect
                // without re-attaching the helper.
                const componentId = d?.entity?.id;
                if (componentId && this.callbacks.isDisabled?.(componentId)) {
                    return;
                }
                const selection = d3.select(event.currentTarget as SVGGElement) as ConnectableComponentSelection;
                if (!this.policy.isValidConnectionSource(selection)) {
                    return;
                }
                const existing = this.resolveCanvasRoot().select<SVGTextElement>('text.add-connect');
                if (!existing.empty()) {
                    return;
                }
                const x: number = d.ui.dimensions.width / 2 - 14;
                const y: number = d.ui.dimensions.height / 2 + 14;
                selection
                    .append<SVGTextElement>('text')
                    .attr('class', 'add-connect')
                    .attr('transform', 'translate(' + x + ', ' + y + ')')
                    .text('\ue834')
                    .datum<AddConnectDatum>({ origX: x, origY: y })
                    .call(this.connect);
            })
            .on('mouseleave.connectable', (event: MouseEvent) => {
                const addConnect = d3.select(event.currentTarget as Element).select('text.add-connect');
                if (!addConnect.empty() && !addConnect.classed('dragging')) {
                    addConnect.remove();
                }
            })
            // mouseover/out: workaround for chrome issue #122746
            .on('mouseover.connectable', (event: MouseEvent) => {
                d3.select(event.currentTarget as Element).classed('hover', () => this.allowConnection(event));
            })
            .on('mouseout.connectable', (event: MouseEvent) => {
                d3.select(event.currentTarget as Element).classed('hover connectable-destination', false);
            });
    }

    /**
     * Detach all listeners and remove the `connectable` class so a flipped
     * `canEdit` state no longer surfaces the connection handle.
     */
    public deactivate<TDatum extends ConnectableComponentDatum>(
        components: d3.Selection<SVGGElement, TDatum, d3.BaseType, unknown>
    ): void {
        components
            .classed('connectable', false)
            .on('mouseenter.connectable', null)
            .on('mouseleave.connectable', null)
            .on('mouseover.connectable', null)
            .on('mouseout.connectable', null);
        const canvasRoot = this.resolveCanvasRoot();
        canvasRoot.selectAll('text.add-connect').remove();
        canvasRoot.selectAll('path.connector').remove();
        canvasRoot.selectAll('g.component').classed('connectable-destination', false);
        this.origin = null;
    }

    /**
     * The drag is permitted only when no shift-modifier is held (otherwise the
     * user is extending a multi-select) and no in-flight selection rectangle
     * is on screen.
     */
    private allowConnection(event: MouseEvent): boolean {
        const canvasRoot = this.resolveCanvasRoot();
        return (
            !event.shiftKey &&
            canvasRoot.select('rect.drag-selection').empty() &&
            canvasRoot.select('rect.component-selection').empty()
        );
    }

    private buildDragBehavior(): d3.DragBehavior<SVGTextElement, AddConnectDatum, AddConnectSubject> {
        const resolveCanvasRoot = this.resolveCanvasRoot;
        const geometry = this.geometry;
        const policy = this.policy;
        const callbacks = this.callbacks;

        return d3
            .drag<SVGTextElement, AddConnectDatum, AddConnectSubject>()
            .subject(function (this: SVGTextElement, event: MouseEvent): AddConnectSubject {
                // currentTarget is only valid during the initiating mousedown,
                // so we capture it via the d3 drag subject here.
                const origin = d3.pointer(event, resolveCanvasRoot());
                return { x: origin[0], y: origin[1], element: this, origin };
            })
            .on('start', (event: AddConnectDragEvent) => {
                event.sourceEvent.stopPropagation();
                this.origin = event.subject.origin;

                const el = event.subject.element;
                const source = d3.select(el.parentNode as SVGGElement) as ConnectableComponentSelection;
                const sourceData = source.datum();

                callbacks.onSelectSource({
                    id: sourceData.entity.id,
                    type: sourceData.ui.componentType
                });

                d3.select(el).classed('dragging', true);

                const canvas = resolveCanvasRoot();
                const canvasNode = canvas.node() as SVGGElement;
                const position = d3.pointer(event, canvasNode);
                const sourcePosition = resolveConnectablePosition(sourceData);

                canvas
                    .insert<SVGPathElement>('path', ':first-child')
                    .datum<ConnectorPathDatum>({
                        sourceId: sourceData.entity.id,
                        sourceWidth: sourceData.ui.dimensions.width,
                        x: sourcePosition.x + sourceData.ui.dimensions.width / 2,
                        y: sourcePosition.y + sourceData.ui.dimensions.height / 2
                    })
                    .attr('class', 'connector')
                    .attr(
                        'd',
                        (pathDatum: ConnectorPathDatum) =>
                            'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + pathDatum.x + ' ' + pathDatum.y
                    );

                d3.select(el).attr('transform', () => 'translate(' + position[0] + ', ' + (position[1] + 20) + ')');
                canvasNode.appendChild(el);
            })
            .on('drag', (event: AddConnectDragEvent) => {
                const el = event.subject.element;
                const canvasRoot = resolveCanvasRoot();
                const position = d3.pointer(event, canvasRoot.node() as SVGGElement);
                const origin = this.origin;
                if (!origin) {
                    return;
                }

                d3.select(el).attr('transform', () => 'translate(' + position[0] + ', ' + (position[1] + 50) + ')');

                // Scope the destination probe + connector lookup to the
                // canvas root so multi-canvas pages don't cross-contaminate.
                // Bind `this` to the DOM element so the policy adapter can
                // inspect classes/data on it.
                const destination = canvasRoot
                    .select<SVGGElement>('g.hover')
                    .classed('connectable-destination', function (this: SVGGElement) {
                        return (
                            (Math.abs(origin[0] - position[0]) > 10 || Math.abs(origin[1] - position[1]) > 10) &&
                            policy.isValidConnectionDestination(d3.select(this) as ConnectableComponentSelection)
                        );
                    }) as ConnectableComponentSelection;

                const connectorPath = canvasRoot.select<SVGPathElement>('path.connector') as d3.Selection<
                    SVGPathElement,
                    ConnectorPathDatum,
                    d3.BaseType,
                    unknown
                >;
                connectorPath
                    .classed('connectable', () => {
                        if (destination.empty()) {
                            return false;
                        }
                        return destination.classed('connectable-destination');
                    })
                    .attr('d', (pathDatum: ConnectorPathDatum) => {
                        if (!destination.empty() && destination.classed('connectable-destination')) {
                            const destinationData = destination.datum();

                            if (pathDatum.sourceId === destinationData.entity.id) {
                                const x: number = pathDatum.x;
                                const y: number = pathDatum.y;
                                const componentOffset: number = pathDatum.sourceWidth / 2 - 50;
                                const xOffset: number = geometry.selfLoopXOffset;
                                const yOffset: number = geometry.selfLoopYOffset;

                                return (
                                    'M' +
                                    (x + componentOffset) +
                                    ' ' +
                                    y +
                                    'L' +
                                    (x + componentOffset + xOffset) +
                                    ' ' +
                                    (y - yOffset) +
                                    'L' +
                                    (x + componentOffset + xOffset) +
                                    ' ' +
                                    (y + yOffset) +
                                    'Z'
                                );
                            }

                            const destinationPosition = resolveConnectablePosition(destinationData);
                            const end: Position = geometry.getPerimeterPoint(pathDatum, {
                                x: destinationPosition.x,
                                y: destinationPosition.y,
                                width: destinationData.ui.dimensions.width,
                                height: destinationData.ui.dimensions.height
                            });
                            return 'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + end.x + ' ' + end.y;
                        }
                        return 'M' + pathDatum.x + ' ' + pathDatum.y + 'L' + position[0] + ' ' + position[1];
                    });
            })
            .on('end', (event: AddConnectDragEvent, d: AddConnectDatum) => {
                event.sourceEvent.stopPropagation();

                const el = event.subject.element;
                const addConnect = d3.select(el);

                const canvasRoot = resolveCanvasRoot();
                const connector = canvasRoot.select<SVGPathElement>('path.connector') as d3.Selection<
                    SVGPathElement,
                    ConnectorPathDatum,
                    d3.BaseType,
                    unknown
                >;
                if (connector.empty()) {
                    addConnect.remove();
                    canvasRoot.selectAll('g.component').classed('connectable-destination', false);
                    this.origin = null;
                    return;
                }
                const connectorData = connector.datum();

                const source = canvasRoot.select<SVGGElement>(
                    '#id-' + connectorData.sourceId
                ) as ConnectableComponentSelection;
                if (source.empty()) {
                    addConnect.remove();
                    connector.remove();
                    canvasRoot.selectAll('g.component').classed('connectable-destination', false);
                    this.origin = null;
                    return;
                }
                const sourceData = source.datum();

                const destination = canvasRoot.select<SVGGElement>(
                    'g.connectable-destination'
                ) as ConnectableComponentSelection;

                if (destination.empty()) {
                    const position = d3.pointer(event, source.node() as SVGGElement);
                    if (
                        position[0] < 0 ||
                        position[0] > sourceData.ui.dimensions.width ||
                        position[1] < 0 ||
                        position[1] > sourceData.ui.dimensions.height
                    ) {
                        addConnect.remove();
                    } else {
                        addConnect
                            .classed('dragging', false)
                            .attr('transform', () => 'translate(' + d.origX + ', ' + d.origY + ')');
                        (source.node() as SVGGElement).appendChild(el);
                    }
                    connector.remove();
                    this.origin = null;
                    return;
                }

                addConnect.remove();
                const destinationData = destination.datum();

                const payload: CreateConnectionPayload = {
                    source: {
                        id: sourceData.entity.id,
                        type: sourceData.ui.componentType,
                        entity: sourceData.entity
                    },
                    destination: {
                        id: destinationData.entity.id,
                        type: destinationData.ui.componentType,
                        entity: destinationData.entity
                    }
                };

                const bends = geometry.calculateInitialBendPoints(sourceData, destinationData);
                if (bends && bends.length > 0) {
                    payload.bends = bends;
                }

                callbacks.onConnectionRequested(payload);
                this.origin = null;
            });
    }
}
