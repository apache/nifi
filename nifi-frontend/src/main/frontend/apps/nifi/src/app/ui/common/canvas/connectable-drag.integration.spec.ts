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

/**
 * Seam tests for the two D3 gesture pipelines that cohabit on a single
 * `g.component` element:
 *
 *   1. `DragUtils.attachComponentDrag` — Phase 0a's component-drag pipeline.
 *      Owns the d3.drag chain that translates the component (and any anchored
 *      bends) and emits `componentsDragEnd` on release. Filter is gated by
 *      `getCanEdit` / `getCanSelect` / `getDisabledIds` and the `extraFilter`
 *      hook.
 *   2. `ConnectableBehaviorHelper` — Phase 0b's connection-handle helper. Owns
 *      the `text.add-connect` glyph that surfaces on hover and the d3.drag
 *      chain attached to *that text element* for drawing a new connection.
 *
 * Both gestures share the same `g.component` host. The four behaviours below
 * are the contracts that keep them from interfering with each other.
 *
 * The fixture intentionally wires *both* helpers against one `g.component`
 * with the same fire-time getters the canvas component would supply, so the
 * tests verify the seam from a single source of truth.
 */

import * as d3 from 'd3';
import { Mock } from 'vitest';
import { ComponentType, Position } from '@nifi/shared';
import {
    ConnectableBehaviorHelper,
    ConnectableCallbacks,
    ConnectablePolicy,
    CreateConnectionPayload
} from './connectable-behavior.helper';
import { CanvasProcessor } from './canvas.types';
import { DragUtils, DraggableDatum } from './utils/drag.utils';
import { CanvasComponentRef } from '../../../state/flow-shared';

const SVG_NS = 'http://www.w3.org/2000/svg';

interface SeamGetters {
    canEdit: boolean;
    canSelect: boolean;
    disabledIds: Set<string>;
    selectedIds: Set<string>;
}

/** Minimal processor-shaped datum used by the seam fixture. */
type SeamComponentDatum = CanvasProcessor & DraggableDatum;

interface SeamCallbacks {
    onClick: Mock<(datum: SeamComponentDatum, event: MouseEvent) => void>;
    onDragEnd: Mock<(delta: Position, movingIds: Set<string>) => void>;
    onSelectSource: Mock<(source: CanvasComponentRef) => void>;
    onConnectionRequested: Mock<(payload: CreateConnectionPayload) => void>;
}

interface SeamFixture {
    canvasContainer: SVGGElement;
    canvasRoot: d3.Selection<SVGGElement, unknown, null, undefined>;
    component: d3.Selection<SVGGElement, SeamComponentDatum, d3.BaseType, unknown>;
    helper: ConnectableBehaviorHelper;
    getters: SeamGetters;
    callbacks: SeamCallbacks;
    cleanup: () => void;
}

/** Private surface that endpoint-reconnect must NOT live on (C5 contract). */
interface EndpointReconnectSurface {
    createEndpointDrag?: unknown;
    onEndpointReconnect?: unknown;
}

function createSeamFixture(
    options: {
        componentId?: string;
        canEdit?: boolean;
        canSelect?: boolean;
        disabledIds?: Set<string>;
        selectedIds?: Set<string>;
        canRead?: boolean;
        canWrite?: boolean;
    } = {}
): SeamFixture {
    const componentId = options.componentId ?? 'p1';

    // Mount `g.canvas` directly under <body> — NOT inside an <svg> element.
    // happy-dom implements `SVGSVGElement.createSVGPoint` but not the
    // matrixTransform / getScreenCTM math d3-selection's `pointer()` falls
    // back to when `ownerSVGElement` is non-null. With the canvas root
    // sitting outside any <svg>, d3.pointer uses `getBoundingClientRect`
    // which happy-dom implements. The renderer specs do the same.
    const canvasContainer = document.createElementNS(SVG_NS, 'g') as SVGGElement;
    canvasContainer.setAttribute('class', 'canvas');
    document.body.appendChild(canvasContainer);

    const canvasRoot = d3.select<SVGGElement, unknown>(canvasContainer);

    const datum: SeamComponentDatum = {
        entity: {
            id: componentId,
            uri: `https://localhost/nifi-api/processors/${componentId}`,
            revision: { version: 1 },
            permissions: { canRead: options.canRead ?? true, canWrite: options.canWrite ?? true },
            operatePermissions: { canRead: true, canWrite: true },
            position: { x: 0, y: 0 },
            inputRequirement: 'INPUT_ALLOWED',
            physicalState: 'STOPPED'
        },
        ui: {
            componentType: ComponentType.Processor,
            dimensions: { width: 100, height: 50 }
        }
    };

    // Mirror the production DOM shape: `g.canvas > g.component[id="id-{id}"]`.
    // The `id-` prefix matters for the connect-drag's `end` lookup of the
    // source via `canvasRoot.select('#id-' + sourceId)`.
    const component: d3.Selection<SVGGElement, SeamComponentDatum, d3.BaseType, unknown> = canvasRoot
        .selectAll<SVGGElement, SeamComponentDatum>('g.component')
        .data([datum])
        .enter()
        .append('g')
        .attr('class', 'component')
        .attr('id', `id-${componentId}`);

    const getters: SeamGetters = {
        canEdit: options.canEdit ?? true,
        canSelect: options.canSelect ?? true,
        disabledIds: options.disabledIds ?? new Set<string>(),
        selectedIds: options.selectedIds ?? new Set<string>()
    };

    const callbacks: SeamCallbacks = {
        onClick: vi.fn<(datum: SeamComponentDatum, event: MouseEvent) => void>(),
        onDragEnd: vi.fn<(delta: Position, movingIds: Set<string>) => void>(),
        onSelectSource: vi.fn<(source: CanvasComponentRef) => void>(),
        onConnectionRequested: vi.fn<(payload: CreateConnectionPayload) => void>()
    };

    // Mirror production renderers: `mousedown.selection` is the single selection
    // authority. `attachComponentDrag` only reads the canonical `selectedIds` signal.
    const handleSelectionClick = (componentDatum: SeamComponentDatum, event: MouseEvent): void => {
        callbacks.onClick(componentDatum, event);

        const id = componentDatum.entity.id;
        if (event.shiftKey) {
            if (getters.selectedIds.has(id)) {
                const next = new Set(getters.selectedIds);
                next.delete(id);
                getters.selectedIds = next;
            } else {
                getters.selectedIds = new Set([...getters.selectedIds, id]);
            }
        } else if (!getters.selectedIds.has(id)) {
            getters.selectedIds = new Set([id]);
        }
    };

    component.on('mousedown.selection', function (event: MouseEvent, d: typeof datum) {
        if (event.button !== 0) {
            return;
        }
        if (!getters.canSelect) {
            return;
        }

        event.stopPropagation();
        handleSelectionClick(d, event);
    });

    DragUtils.attachComponentDrag(component, {
        resolveCanvasRoot: () => canvasRoot,
        getCanEdit: () => getters.canEdit,
        getCanSelect: () => getters.canSelect,
        getDisabledIds: () => getters.disabledIds,
        getSelectedIds: () => getters.selectedIds,
        onDragEnd: (delta, movingIds) => callbacks.onDragEnd(delta, movingIds)
    });

    const policy: ConnectablePolicy = {
        isValidConnectionSource: vi.fn().mockReturnValue(true),
        isValidConnectionDestination: vi.fn().mockReturnValue(true)
    };

    const connectableCallbacks: ConnectableCallbacks = {
        onSelectSource: (source: CanvasComponentRef) => callbacks.onSelectSource(source),
        onConnectionRequested: (payload: CreateConnectionPayload) => callbacks.onConnectionRequested(payload),
        isDisabled: (id: string) => getters.disabledIds.has(id)
    };

    const helper = new ConnectableBehaviorHelper(policy, connectableCallbacks, () => canvasRoot);
    helper.activate(component);

    return {
        canvasContainer,
        canvasRoot,
        component,
        helper,
        getters,
        callbacks,
        cleanup: () => canvasContainer.remove()
    };
}

function getAddConnect(
    component: d3.Selection<SVGGElement, SeamComponentDatum, d3.BaseType, unknown>
): SVGTextElement | null {
    const sel = component.select<SVGTextElement>('text.add-connect');
    return sel.empty() ? null : (sel.node() as SVGTextElement);
}

function hover(
    component: d3.Selection<SVGGElement, SeamComponentDatum, d3.BaseType, unknown>,
    opts: { shiftKey?: boolean } = {}
): void {
    component.node()!.dispatchEvent(new MouseEvent('mouseenter', { bubbles: false, ...opts }));
}

function mousedown(target: Element, opts: MouseEventInit = {}): void {
    target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window, ...opts }));
}

function mouseup(target: Element | Window = window, opts: MouseEventInit = {}): void {
    (target as EventTarget).dispatchEvent(
        new MouseEvent('mouseup', { bubbles: true, button: 0, view: window, ...opts })
    );
}

describe('Connectable + Drag pipeline seam', () => {
    describe('C1 - connect-handle grab does not start a component drag', () => {
        it('mousedown on text.add-connect does not invoke the component drag onDragEnd', () => {
            const fixture = createSeamFixture();
            try {
                hover(fixture.component);
                const handle = getAddConnect(fixture.component);
                expect(handle).not.toBeNull();

                // Grab the connect handle and release. The component drag's
                // d3.drag chain shares the bubble path from the handle up to
                // its host `g.component`, so it must NOT engage when the
                // gesture originates on `text.add-connect`.
                mousedown(handle as Element);
                mouseup();

                expect(fixture.callbacks.onDragEnd).not.toHaveBeenCalled();
            } finally {
                fixture.cleanup();
            }
        });

        it('component-body grab still fires componentsDragEnd (control)', () => {
            // Sibling control: prove the seam is not just always rejecting
            // — the same fixture still allows a body grab.
            const fixture = createSeamFixture();
            try {
                mousedown(fixture.component.node()!);
                mouseup();

                expect(fixture.callbacks.onDragEnd).toHaveBeenCalledTimes(1);
            } finally {
                fixture.cleanup();
            }
        });
    });

    describe('C2 - shift handling is split across the two gestures', () => {
        it('shift+hover does not surface the connect handle', () => {
            const fixture = createSeamFixture();
            try {
                hover(fixture.component, { shiftKey: true });

                expect(getAddConnect(fixture.component)).toBeNull();
            } finally {
                fixture.cleanup();
            }
        });

        it('shift+component-body grab still fires componentsDragEnd (snap-off path)', () => {
            // The component drag's filter only blocks `event.ctrlKey` and
            // `event.button !== 0` — shift is forwarded through to the gesture
            // so `snapEnabled = !shiftKey` flips off without rejecting the drag.
            const fixture = createSeamFixture();
            try {
                mousedown(fixture.component.node()!, { shiftKey: true });
                mouseup(window, { shiftKey: true });

                expect(fixture.callbacks.onDragEnd).toHaveBeenCalledTimes(1);
            } finally {
                fixture.cleanup();
            }
        });
    });

    describe('C3 - canEdit / canSelect runtime toggle lifts both behaviors', () => {
        it('flipping canEdit + deactivate() both suppresses the handle and rejects the drag', () => {
            const fixture = createSeamFixture();
            try {
                // Sanity: with canEdit/canSelect on, both interactions work.
                hover(fixture.component);
                expect(getAddConnect(fixture.component)).not.toBeNull();
                // Remove the handle so the next hover is a fresh evaluation.
                fixture.component.node()!.dispatchEvent(new MouseEvent('mouseleave', { bubbles: false }));

                fixture.getters.canEdit = false;
                fixture.getters.canSelect = false;
                fixture.helper.deactivate(fixture.component);

                hover(fixture.component);
                expect(getAddConnect(fixture.component)).toBeNull();

                mousedown(fixture.component.node()!);
                mouseup();
                expect(fixture.callbacks.onDragEnd).not.toHaveBeenCalled();
            } finally {
                fixture.cleanup();
            }
        });

        it('flipping canEdit back on + re-activate() restores both behaviors without re-mounting', () => {
            const fixture = createSeamFixture();
            try {
                fixture.getters.canEdit = false;
                fixture.getters.canSelect = false;
                fixture.helper.deactivate(fixture.component);

                fixture.getters.canEdit = true;
                fixture.getters.canSelect = true;
                fixture.helper.activate(fixture.component);

                hover(fixture.component);
                expect(getAddConnect(fixture.component)).not.toBeNull();

                fixture.component.node()!.dispatchEvent(new MouseEvent('mouseleave', { bubbles: false }));
                mousedown(fixture.component.node()!);
                mouseup();
                expect(fixture.callbacks.onDragEnd).toHaveBeenCalledTimes(1);
            } finally {
                fixture.cleanup();
            }
        });
    });

    describe('C4 - saving processor cannot be a connection source', () => {
        it('isDisabled short-circuits mouseenter so text.add-connect never appears', () => {
            // Symmetric with the per-id `getDisabledIds` filter the component
            // drag uses: when a component is locked by an in-flight save, the
            // hover gate refuses to spawn the connection-handle glyph.
            const disabledIds = new Set(['p1']);
            const fixture = createSeamFixture({ disabledIds });
            try {
                hover(fixture.component);

                expect(getAddConnect(fixture.component)).toBeNull();
            } finally {
                fixture.cleanup();
            }
        });

        it('isDisabled toggle takes effect at fire-time without re-attach', () => {
            const fixture = createSeamFixture();
            try {
                hover(fixture.component);
                expect(getAddConnect(fixture.component)).not.toBeNull();
                fixture.component.node()!.dispatchEvent(new MouseEvent('mouseleave', { bubbles: false }));

                fixture.getters.disabledIds.add('p1');

                hover(fixture.component);
                expect(getAddConnect(fixture.component)).toBeNull();
            } finally {
                fixture.cleanup();
            }
        });

        it('component drag is also blocked while disabled (mirrors the saving-state contract)', () => {
            const fixture = createSeamFixture({ disabledIds: new Set(['p1']) });
            try {
                mousedown(fixture.component.node()!);
                mouseup();

                expect(fixture.callbacks.onDragEnd).not.toHaveBeenCalled();
            } finally {
                fixture.cleanup();
            }
        });
    });

    describe('C5 - mousedown.selection is the single selection authority', () => {
        // Shift+click on an already-selected component must deselect it exactly
        // once: `mousedown.selection` removes the id, and drag `start` must not
        // re-invoke onClick afterward and re-add it (double-toggle).
        it('shift+click on an already-selected component deselects it (onClick fires once)', () => {
            const fixture = createSeamFixture({ selectedIds: new Set(['p1', 'p2']) });
            try {
                mousedown(fixture.component.node()!, { shiftKey: true });
                mouseup(window, { shiftKey: true });

                expect(fixture.callbacks.onClick).toHaveBeenCalledTimes(1);
                expect(fixture.getters.selectedIds.has('p1')).toBe(false);
                expect(fixture.getters.selectedIds.has('p2')).toBe(true);
            } finally {
                fixture.cleanup();
            }
        });

        it('grab of an unselected component selects via mousedown.selection before drag start', () => {
            const fixture = createSeamFixture({ selectedIds: new Set(['other']) });
            try {
                mousedown(fixture.component.node()!);

                expect(fixture.getters.selectedIds.has('p1')).toBe(true);
                expect(fixture.getters.selectedIds.has('other')).toBe(false);

                mouseup();
                expect(fixture.callbacks.onDragEnd).toHaveBeenCalledTimes(1);
            } finally {
                fixture.cleanup();
            }
        });
    });

    describe('C5 - endpoint reconnect is bespoke (not routed through ConnectableBehaviorHelper)', () => {
        it('ConnectableBehaviorHelper exposes no endpoint-reconnect drag API', () => {
            const fixture = createSeamFixture();
            try {
                expect(typeof (fixture.helper as unknown as EndpointReconnectSurface).createEndpointDrag).toBe(
                    'undefined'
                );
                expect(typeof (fixture.helper as unknown as EndpointReconnectSurface).onEndpointReconnect).toBe(
                    'undefined'
                );
            } finally {
                fixture.cleanup();
            }
        });

        it('disabling canEdit suppresses the connection-handle without affecting component drag contract', () => {
            const fixture = createSeamFixture();
            try {
                fixture.getters.canEdit = false;
                fixture.helper.deactivate(fixture.component);

                hover(fixture.component);
                expect(getAddConnect(fixture.component)).toBeNull();

                fixture.getters.canEdit = true;
                fixture.getters.canSelect = true;
                fixture.helper.activate(fixture.component);

                mousedown(fixture.component.node()!);
                mouseup();
                expect(fixture.callbacks.onDragEnd).toHaveBeenCalledTimes(1);
            } finally {
                fixture.cleanup();
            }
        });
    });
});
