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
import { ComponentType, ConnectionEntity, LabelEntity } from '@nifi/shared';
import {
    ConnectableBehaviorHelper,
    ConnectableCallbacks,
    ConnectablePolicy,
    CreateConnectionPayload,
    resolveConnectablePosition
} from './connectable-behavior.helper';
import { CanvasProcessor } from './canvas.types';
import {
    CanvasComponentRef,
    CanvasConnectableEntity,
    ConnectionEndpointReconnectDestination
} from '../../../state/flow-shared';

function createCanvasFixture() {
    // Mount outside an <svg> so happy-dom's d3.pointer fallback uses
    // getBoundingClientRect instead of the unimplemented SVG matrix APIs.
    const canvas = document.createElementNS('http://www.w3.org/2000/svg', 'g') as SVGGElement;
    canvas.setAttribute('class', 'canvas');
    document.body.appendChild(canvas);
    const root = d3.select<SVGGElement, unknown>(canvas);

    return {
        root,
        cleanup: () => {
            canvas.remove();
        }
    };
}

function makePolicy(overrides: Partial<ConnectablePolicy> = {}): ConnectablePolicy {
    return {
        isValidConnectionSource: vi.fn().mockReturnValue(true),
        isValidConnectionDestination: vi.fn().mockReturnValue(true),
        ...overrides
    };
}

function makeCallbacks(): ConnectableCallbacks & {
    onSelectSource: ReturnType<typeof vi.fn>;
    onConnectionRequested: ReturnType<typeof vi.fn>;
} {
    return {
        onSelectSource: vi.fn<(source: CanvasComponentRef) => void>(),
        onConnectionRequested: vi.fn<(payload: CreateConnectionPayload) => void>()
    };
}

function makeProcessorDatum(id = 'p1', position = { x: 0, y: 0 }): CanvasProcessor {
    return {
        entity: {
            id,
            uri: `https://localhost/nifi-api/processors/${id}`,
            revision: { version: 1 },
            permissions: { canRead: true, canWrite: true },
            operatePermissions: { canRead: true, canWrite: true },
            position,
            inputRequirement: 'INPUT_ALLOWED',
            physicalState: 'STOPPED'
        },
        ui: {
            componentType: ComponentType.Processor,
            dimensions: { width: 100, height: 50 }
        }
    };
}

describe('ConnectableBehaviorHelper', () => {
    it('excludes labels and connections from endpoint entity contracts', () => {
        expectTypeOf<LabelEntity>().not.toExtend<CanvasConnectableEntity>();
        expectTypeOf<ConnectionEntity>().not.toExtend<CanvasConnectableEntity>();
        expectTypeOf<CreateConnectionPayload['source']['entity']>().toEqualTypeOf<CanvasConnectableEntity>();
        expectTypeOf<ConnectionEndpointReconnectDestination['entity']>().toEqualTypeOf<CanvasConnectableEntity>();
    });

    it('constructs without throwing', () => {
        const fixture = createCanvasFixture();
        try {
            expect(
                () => new ConnectableBehaviorHelper(makePolicy(), makeCallbacks(), () => fixture.root)
            ).not.toThrow();
        } finally {
            fixture.cleanup();
        }
    });

    describe('activate', () => {
        it('adds the connectable class and registers hover listeners', () => {
            const fixture = createCanvasFixture();
            try {
                const helper = new ConnectableBehaviorHelper(makePolicy(), makeCallbacks(), () => fixture.root);
                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);

                components.each(function () {
                    const sel = d3.select(this);
                    expect(sel.classed('connectable')).toBe(true);
                    expect(sel.on('mouseenter.connectable')).toBeDefined();
                    expect(sel.on('mouseleave.connectable')).toBeDefined();
                });
            } finally {
                fixture.cleanup();
            }
        });

        it('appends an add-connect glyph on mouseenter when source is valid', () => {
            const fixture = createCanvasFixture();
            try {
                const policy = makePolicy({ isValidConnectionSource: vi.fn().mockReturnValue(true) });
                const helper = new ConnectableBehaviorHelper(policy, makeCallbacks(), () => fixture.root);

                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);

                const node = components.node() as Element;
                node.dispatchEvent(new MouseEvent('mouseenter', { bubbles: false }));

                expect(policy.isValidConnectionSource).toHaveBeenCalled();
                expect(d3.select(node).select('text.add-connect').empty()).toBe(false);
            } finally {
                fixture.cleanup();
            }
        });

        it('does not append the add-connect glyph when source is invalid', () => {
            const fixture = createCanvasFixture();
            try {
                const policy = makePolicy({ isValidConnectionSource: vi.fn().mockReturnValue(false) });
                const helper = new ConnectableBehaviorHelper(policy, makeCallbacks(), () => fixture.root);

                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);

                const node = components.node() as Element;
                node.dispatchEvent(new MouseEvent('mouseenter', { bubbles: false }));

                expect(d3.select(node).select('text.add-connect').empty()).toBe(true);
            } finally {
                fixture.cleanup();
            }
        });

        it('removes the add-connect glyph on mouseleave when not currently dragging', () => {
            const fixture = createCanvasFixture();
            try {
                const helper = new ConnectableBehaviorHelper(makePolicy(), makeCallbacks(), () => fixture.root);

                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);

                const node = components.node() as Element;
                node.dispatchEvent(new MouseEvent('mouseenter'));
                expect(d3.select(node).select('text.add-connect').empty()).toBe(false);

                node.dispatchEvent(new MouseEvent('mouseleave'));
                expect(d3.select(node).select('text.add-connect').empty()).toBe(true);
            } finally {
                fixture.cleanup();
            }
        });

        it('skips activation when shift is held (extending a multi-select)', () => {
            const fixture = createCanvasFixture();
            try {
                const policy = makePolicy();
                const helper = new ConnectableBehaviorHelper(policy, makeCallbacks(), () => fixture.root);

                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);

                const node = components.node() as Element;
                node.dispatchEvent(new MouseEvent('mouseenter', { shiftKey: true }));

                expect(policy.isValidConnectionSource).not.toHaveBeenCalled();
                expect(d3.select(node).select('text.add-connect').empty()).toBe(true);
            } finally {
                fixture.cleanup();
            }
        });

        it('skips activation while a drag-selection rectangle is active', () => {
            const fixture = createCanvasFixture();
            try {
                fixture.root.append('rect').attr('class', 'drag-selection');
                const policy = makePolicy();
                const helper = new ConnectableBehaviorHelper(policy, makeCallbacks(), () => fixture.root);

                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);

                const node = components.node() as Element;
                node.dispatchEvent(new MouseEvent('mouseenter'));

                expect(policy.isValidConnectionSource).not.toHaveBeenCalled();
            } finally {
                fixture.cleanup();
            }
        });

        it('forwards the full source and destination entities after a successful connect drag', () => {
            const fixture = createCanvasFixture();
            try {
                const callbacks = makeCallbacks();
                const helper = new ConnectableBehaviorHelper(makePolicy(), callbacks, () => fixture.root);
                const sourceDatum = makeProcessorDatum('source');
                const destinationDatum = makeProcessorDatum('destination', { x: 200, y: 0 });
                const components = fixture.root
                    .selectAll('g.component')
                    .data([sourceDatum, destinationDatum])
                    .enter()
                    .append('g')
                    .attr('class', 'component')
                    .attr('id', (datum) => `id-${datum.entity.id}`);

                helper.activate(components);

                const source = components.filter((datum) => datum.entity.id === sourceDatum.entity.id);
                const destination = components.filter((datum) => datum.entity.id === destinationDatum.entity.id);
                source.node()?.dispatchEvent(new MouseEvent('mouseenter', { bubbles: false }));

                const addConnect = source.select<SVGTextElement>('text.add-connect');
                addConnect
                    .node()
                    ?.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));
                destination.classed('connectable-destination', true);
                window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));

                expect(callbacks.onConnectionRequested).toHaveBeenCalledWith({
                    source: {
                        id: sourceDatum.entity.id,
                        type: ComponentType.Processor,
                        entity: sourceDatum.entity
                    },
                    destination: {
                        id: destinationDatum.entity.id,
                        type: ComponentType.Processor,
                        entity: destinationDatum.entity
                    }
                });
            } finally {
                fixture.cleanup();
            }
        });

        it('anchors the connector preview to the source currentPosition without modifying the entity', () => {
            const fixture = createCanvasFixture();
            try {
                const helper = new ConnectableBehaviorHelper(makePolicy(), makeCallbacks(), () => fixture.root);
                const sourceDatum = makeProcessorDatum('source', { x: 10, y: 20 });
                sourceDatum.ui.currentPosition = { x: 110, y: 120 };
                const components = fixture.root
                    .selectAll('g.component')
                    .data([sourceDatum])
                    .enter()
                    .append('g')
                    .attr('class', 'component')
                    .attr('id', (datum) => `id-${datum.entity.id}`);

                helper.activate(components);
                components.node()?.dispatchEvent(new MouseEvent('mouseenter', { bubbles: false }));
                components
                    .select<SVGTextElement>('text.add-connect')
                    .node()
                    ?.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));

                expect(fixture.root.select<SVGPathElement>('path.connector').datum()).toMatchObject({
                    x: 160,
                    y: 145
                });
                expect(sourceDatum.entity.position).toEqual({ x: 10, y: 20 });

                window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));
            } finally {
                fixture.cleanup();
            }
        });

        it('resolves optimistic geometry without modifying the entity position', () => {
            const datum = makeProcessorDatum('destination', { x: 200, y: 0 });
            datum.ui.currentPosition = { x: 300, y: 40 };

            expect(resolveConnectablePosition(datum)).toEqual({ x: 300, y: 40 });
            expect(datum.entity.position).toEqual({ x: 200, y: 0 });

            delete datum.ui.currentPosition;
            expect(resolveConnectablePosition(datum)).toEqual({ x: 200, y: 0 });
        });
    });

    describe('deactivate', () => {
        it('removes the connectable class and tears down listeners', () => {
            const fixture = createCanvasFixture();
            try {
                const helper = new ConnectableBehaviorHelper(makePolicy(), makeCallbacks(), () => fixture.root);
                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);
                helper.deactivate(components);

                components.each(function () {
                    const sel = d3.select(this);
                    expect(sel.classed('connectable')).toBe(false);
                    // Each listener is registered + torn down under the
                    // `.connectable` namespace; an inconsistent namespace
                    // (e.g. `mouseout.connection`) would leak past
                    // `deactivate(...)` because the targeted `.on(name, null)`
                    // wouldn't match.
                    expect(sel.on('mouseenter.connectable')).toBeUndefined();
                    expect(sel.on('mouseleave.connectable')).toBeUndefined();
                    expect(sel.on('mouseover.connectable')).toBeUndefined();
                    expect(sel.on('mouseout.connectable')).toBeUndefined();
                });
            } finally {
                fixture.cleanup();
            }
        });

        it('prevents the add-connect glyph from appearing after deactivation', () => {
            const fixture = createCanvasFixture();
            try {
                const policy = makePolicy();
                const helper = new ConnectableBehaviorHelper(policy, makeCallbacks(), () => fixture.root);

                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');

                helper.activate(components);
                helper.deactivate(components);

                const node = components.node() as Element;
                node.dispatchEvent(new MouseEvent('mouseenter'));

                expect(policy.isValidConnectionSource).not.toHaveBeenCalled();
                expect(d3.select(node).select('text.add-connect').empty()).toBe(true);
            } finally {
                fixture.cleanup();
            }
        });

        it('removes an existing handle and permits clean activation by a replacement helper', () => {
            const fixture = createCanvasFixture();
            try {
                const firstPolicy = makePolicy();
                const secondPolicy = makePolicy();
                const firstHelper = new ConnectableBehaviorHelper(firstPolicy, makeCallbacks(), () => fixture.root);
                const secondHelper = new ConnectableBehaviorHelper(secondPolicy, makeCallbacks(), () => fixture.root);
                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component');
                const node = components.node() as Element;

                firstHelper.activate(components);
                node.dispatchEvent(new MouseEvent('mouseenter'));
                expect(d3.select(node).select('text.add-connect').empty()).toBe(false);

                firstHelper.deactivate(components);
                expect(d3.select(node).select('text.add-connect').empty()).toBe(true);

                secondHelper.activate(components);
                node.dispatchEvent(new MouseEvent('mouseenter'));

                expect(firstPolicy.isValidConnectionSource).toHaveBeenCalledTimes(1);
                expect(secondPolicy.isValidConnectionSource).toHaveBeenCalledTimes(1);
                expect(d3.select(node).selectAll('text.add-connect').size()).toBe(1);
            } finally {
                fixture.cleanup();
            }
        });

        it('safely tears down a create-connection drag that is already in flight', () => {
            const fixture = createCanvasFixture();
            try {
                const callbacks = makeCallbacks();
                const helper = new ConnectableBehaviorHelper(makePolicy(), callbacks, () => fixture.root);
                const components = fixture.root
                    .selectAll('g.component')
                    .data([makeProcessorDatum()])
                    .enter()
                    .append('g')
                    .attr('class', 'component')
                    .attr('id', (datum) => `id-${datum.entity.id}`);
                helper.activate(components);
                const component = components.node()!;
                component.dispatchEvent(new MouseEvent('mouseenter'));
                const handle = component.querySelector('text.add-connect')!;
                handle.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));

                expect(fixture.root.select('path.connector').empty()).toBe(false);
                expect(fixture.root.select('text.add-connect').empty()).toBe(false);

                helper.deactivate(components);

                expect(fixture.root.select('path.connector').empty()).toBe(true);
                expect(fixture.root.select('text.add-connect').empty()).toBe(true);
                expect(() =>
                    window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }))
                ).not.toThrow();
                expect(callbacks.onConnectionRequested).not.toHaveBeenCalled();
            } finally {
                fixture.cleanup();
            }
        });
    });
});
