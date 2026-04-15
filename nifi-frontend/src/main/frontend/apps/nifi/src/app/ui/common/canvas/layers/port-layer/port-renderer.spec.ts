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
import { PortRenderer } from './port-renderer';
import { PortRenderContext } from '../render-context.types';
import { CanvasPort } from '../../canvas.types';
import { ComponentType } from '@nifi/shared';

/**
 * Test setup options for PortRenderer tests
 */
interface SetupOptions {
    ports?: CanvasPort[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledPortIds?: Set<string>;
    callbacks?: Partial<PortRenderContext['callbacks']>;
}

/**
 * Creates a mock port entity for testing
 */
function createMockPort(
    overrides: {
        entity?: {
            id?: string;
            position?: { x: number; y: number };
            permissions?: { canRead?: boolean; canWrite?: boolean };
            allowRemoteAccess?: boolean;
            component?: {
                name?: string;
                comments?: string;
                validationErrors?: string[];
            };
            status?: {
                aggregateSnapshot?: {
                    runStatus?: string;
                };
                transmitting?: boolean;
            };
            bulletins?: any[];
        };
        ui?: {
            componentType?: ComponentType.InputPort | ComponentType.OutputPort;
            dimensions?: { width: number; height: number };
            currentPosition?: { x: number; y: number };
        };
    } = {}
): CanvasPort {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    // Determine height based on allowRemoteAccess (80 for remote, 48 for local)
    const isRemote = entityOverrides.allowRemoteAccess ?? false;
    const defaultHeight = isRemote ? 80 : 48;

    const defaultPort: CanvasPort = {
        entity: {
            id: entityOverrides.id ?? 'port-1',
            position: entityOverrides.position ?? { x: 100, y: 200 },
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            },
            allowRemoteAccess: isRemote,
            component: {
                name: entityOverrides.component?.name ?? 'Test Port',
                comments: entityOverrides.component?.comments ?? '',
                validationErrors: entityOverrides.component?.validationErrors ?? []
            },
            status: {
                aggregateSnapshot: {
                    runStatus: entityOverrides.status?.aggregateSnapshot?.runStatus ?? 'Stopped'
                },
                transmitting: entityOverrides.status?.transmitting ?? false
            },
            bulletins: entityOverrides.bulletins ?? []
        },
        ui: {
            componentType: uiOverrides.componentType ?? ComponentType.InputPort,
            dimensions: uiOverrides.dimensions ?? { width: 240, height: defaultHeight },
            currentPosition: uiOverrides.currentPosition
        }
    };

    return defaultPort;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): PortRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('port-container');
    document.body.appendChild(container);

    return {
        containerSelection: d3.select(container),
        ports: options.ports || [],
        canSelect: options.canSelect ?? true,
        disabledPortIds: options.disabledPortIds,
        getCanEdit: () => options.canEdit ?? true,
        textEllipsis: {
            applyEllipsis: vi.fn((selection, text, _className) => {
                selection.text(text);
            })
        } as any,
        formatUtils: {} as any,
        componentUtils: {
            bulletins: vi.fn(),
            activeThreadCount: vi.fn(),
            comments: vi.fn(),
            canvasTooltip: vi.fn(),
            resetCanvasTooltip: vi.fn()
        } as any,
        callbacks: {
            onClick: options.callbacks?.onClick,
            onDoubleClick: options.callbacks?.onDoubleClick,
            onDragEnd: options.callbacks?.onDragEnd
        }
    };
}

/**
 * SIFERS-style async setup function
 */
async function setup(options: SetupOptions = {}) {
    const context = createMockContext(options);

    // Render ports
    PortRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getPortElements: () =>
            context.containerSelection.selectAll<SVGGElement, CanvasPort>('g.input-port, g.output-port'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.port-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('PortRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.port-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new ports', () => {
            it('should create input port group elements', async () => {
                const port = createMockPort({
                    ui: { componentType: ComponentType.InputPort }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const elements = getPortElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-port-1');
                expect(elements.classed('input-port')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create output port group elements', async () => {
                const port = createMockPort({
                    entity: { id: 'output-port-1' },
                    ui: { componentType: ComponentType.OutputPort }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const elements = getPortElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-output-port-1');
                expect(elements.classed('output-port')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create border and body rectangles', async () => {
                const port = createMockPort();
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portEl = getPortElements();
                expect(portEl.select('rect.border').empty()).toBe(false);
                expect(portEl.select('rect.body').empty()).toBe(false);

                cleanup();
            });

            it('should create remote banner (hidden for local ports)', async () => {
                const port = createMockPort({
                    entity: { allowRemoteAccess: false }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portEl = getPortElements();
                const remoteBanner = portEl.select('rect.remote-banner');
                expect(remoteBanner.empty()).toBe(false);
                expect(remoteBanner.classed('hidden')).toBe(true);

                cleanup();
            });

            it('should create remote banner (visible for remote ports)', async () => {
                const port = createMockPort({
                    entity: { allowRemoteAccess: true },
                    ui: { dimensions: { width: 240, height: 80 } }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portEl = getPortElements();
                const remoteBanner = portEl.select('rect.remote-banner');
                expect(remoteBanner.empty()).toBe(false);
                expect(remoteBanner.classed('hidden')).toBe(false);

                cleanup();
            });

            it('should create port icon with correct character for input port', async () => {
                const port = createMockPort({
                    ui: { componentType: ComponentType.InputPort }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portIcon = getPortElements().select('text.port-icon');
                expect(portIcon.empty()).toBe(false);
                expect(portIcon.text()).toBe('\ue832'); // Input port icon

                cleanup();
            });

            it('should create port icon with correct character for output port', async () => {
                const port = createMockPort({
                    ui: { componentType: ComponentType.OutputPort }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portIcon = getPortElements().select('text.port-icon');
                expect(portIcon.empty()).toBe(false);
                expect(portIcon.text()).toBe('\ue833'); // Output port icon

                cleanup();
            });

            it('should create port name element', async () => {
                const port = createMockPort({
                    entity: { component: { name: 'My Test Port' } }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portName = getPortElements().select('text.port-name');
                expect(portName.empty()).toBe(false);
                expect(portName.text()).toBe('My Test Port');

                cleanup();
            });

            it('should create multiple ports', async () => {
                const port1 = createMockPort({ entity: { id: 'port-1' } });
                const port2 = createMockPort({ entity: { id: 'port-2' } });
                const port3 = createMockPort({
                    entity: { id: 'port-3' },
                    ui: { componentType: ComponentType.OutputPort }
                });

                const { getPortElements, cleanup } = await setup({
                    ports: [port1, port2, port3]
                });

                expect(getPortElements().size()).toBe(3);

                cleanup();
            });
        });

        describe('UPDATE phase - positioning and styling', () => {
            it('should position ports using transform', async () => {
                const port = createMockPort({
                    entity: { position: { x: 150, y: 250 } }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const transform = getPortElements().attr('transform');
                expect(transform).toBe('translate(150, 250)');

                cleanup();
            });

            it('should use ui.currentPosition when set (during drag)', async () => {
                const port = createMockPort({
                    entity: { position: { x: 100, y: 100 } },
                    ui: {
                        dimensions: { width: 240, height: 48 },
                        currentPosition: { x: 200, y: 300 }
                    }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const transform = getPortElements().attr('transform');
                expect(transform).toBe('translate(200, 300)');

                cleanup();
            });

            it('should set dimensions on border and body', async () => {
                const port = createMockPort({
                    ui: { dimensions: { width: 240, height: 80 } }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portEl = getPortElements();
                expect(portEl.select('rect.border').attr('height')).toBe('80');
                expect(portEl.select('rect.body').attr('height')).toBe('80');

                cleanup();
            });

            it('should apply disabled styling when port is in disabledPortIds', async () => {
                const port = createMockPort({ entity: { id: 'port-disabled' } });
                const { getPortElements, cleanup } = await setup({
                    ports: [port],
                    disabledPortIds: new Set(['port-disabled'])
                });

                const portEl = getPortElements();
                expect(portEl.classed('disabled')).toBe(true);
                expect(portEl.style('opacity')).toBe('0.6');
                expect(portEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const port = createMockPort();
                const { getPortElements, cleanup } = await setup({
                    ports: [port],
                    canSelect: true
                });

                expect(getPortElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const port = createMockPort();
                const { getPortElements, cleanup } = await setup({
                    ports: [port],
                    canSelect: false
                });

                expect(getPortElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const port = createMockPort({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                const portEl = getPortElements();
                expect(portEl.select('rect.border').classed('unauthorized')).toBe(true);
                expect(portEl.select('rect.body').classed('unauthorized')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - visible ports with details', () => {
            it('should create details group when port is visible', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                // Mark port as visible
                getPortElements().classed('visible', true);

                // Re-render to trigger details creation
                PortRenderer.render(context);

                expect(getPortElements().select('g.port-details').empty()).toBe(false);

                cleanup();
            });

            it('should create run status icon in details', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const details = getPortElements().select('g.port-details');
                expect(details.select('text.run-status-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create bulletin elements in details', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const details = getPortElements().select('g.port-details');
                expect(details.select('rect.bulletin-background').empty()).toBe(false);
                expect(details.select('text.bulletin-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create active thread count elements in details', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const details = getPortElements().select('g.port-details');
                expect(details.select('text.active-thread-count-icon').empty()).toBe(false);
                expect(details.select('text.active-thread-count').empty()).toBe(false);

                cleanup();
            });

            it('should create comment icon in details', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const details = getPortElements().select('g.port-details');
                expect(details.select('text.component-comments').empty()).toBe(false);

                cleanup();
            });

            it('should create transmission icon for remote ports', async () => {
                const port = createMockPort({
                    entity: { allowRemoteAccess: true },
                    ui: { dimensions: { width: 240, height: 80 } }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const details = getPortElements().select('g.port-details');
                const transmissionIcon = details.select('text.port-transmission-icon');
                expect(transmissionIcon.empty()).toBe(false);
                expect(transmissionIcon.classed('hidden')).toBe(false);

                cleanup();
            });

            it('should hide transmission icon for local ports', async () => {
                const port = createMockPort({
                    entity: { allowRemoteAccess: false }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const details = getPortElements().select('g.port-details');
                const transmissionIcon = details.select('text.port-transmission-icon');
                expect(transmissionIcon.classed('hidden')).toBe(true);

                cleanup();
            });

            it('should remove details when port becomes not visible', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                // First make visible and render details
                getPortElements().classed('visible', true);
                PortRenderer.render(context);
                expect(getPortElements().select('g.port-details').empty()).toBe(false);

                // Now make not visible and re-render
                getPortElements().classed('visible', false);
                PortRenderer.render(context);
                expect(getPortElements().select('g.port-details').empty()).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - run status styling', () => {
            it('should show running status with success color', async () => {
                const port = createMockPort({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Running' }
                        }
                    }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const runStatusIcon = getPortElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('running')).toBe(true);
                expect(runStatusIcon.classed('success-color-default')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf04b'); // fa-play

                cleanup();
            });

            it('should show stopped status with error color', async () => {
                const port = createMockPort({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Stopped' }
                        }
                    }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const runStatusIcon = getPortElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('stopped')).toBe(true);
                expect(runStatusIcon.classed('error-color-variant')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf04d'); // fa-stop

                cleanup();
            });

            it('should show invalid status with caution color', async () => {
                const port = createMockPort({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Invalid' }
                        }
                    }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const runStatusIcon = getPortElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('invalid')).toBe(true);
                expect(runStatusIcon.classed('caution-color')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf071'); // fa-warning

                cleanup();
            });

            it('should show disabled status with flowfont', async () => {
                const port = createMockPort({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Disabled' }
                        }
                    }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const runStatusIcon = getPortElements().select('text.run-status-icon');
                expect(runStatusIcon.empty()).toBe(false);
                expect(runStatusIcon.text()).toBe('\ue802');

                cleanup();
            });
        });

        describe('UPDATE phase - transmission icon for remote ports', () => {
            it('should show transmitting icon when port is transmitting', async () => {
                const port = createMockPort({
                    entity: {
                        allowRemoteAccess: true,
                        status: { transmitting: true }
                    },
                    ui: { dimensions: { width: 240, height: 80 } }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const transmissionIcon = getPortElements().select('text.port-transmission-icon');
                expect(transmissionIcon.classed('transmitting')).toBe(true);
                expect(transmissionIcon.classed('success-color-variant')).toBe(true);
                expect(transmissionIcon.text()).toBe('\uf140'); // Satellite icon

                cleanup();
            });

            it('should show not-transmitting icon when port is not transmitting', async () => {
                const port = createMockPort({
                    entity: {
                        allowRemoteAccess: true,
                        status: { transmitting: false }
                    },
                    ui: { dimensions: { width: 240, height: 80 } }
                });
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                getPortElements().classed('visible', true);
                PortRenderer.render(context);

                const transmissionIcon = getPortElements().select('text.port-transmission-icon');
                expect(transmissionIcon.classed('not-transmitting')).toBe(true);
                expect(transmissionIcon.classed('neutral-color')).toBe(true);
                expect(transmissionIcon.text()).toBe('\ue80a'); // flowfont icon

                cleanup();
            });
        });

        describe('UPDATE phase - port icon position', () => {
            it('should position port icon lower for remote ports than local ports', async () => {
                const localPort = createMockPort({
                    entity: { id: 'local-port', allowRemoteAccess: false }
                });
                const remotePort = createMockPort({
                    entity: { id: 'remote-port', allowRemoteAccess: true },
                    ui: { dimensions: { width: 240, height: 80 } }
                });

                const { context, cleanup } = await setup({
                    ports: [localPort, remotePort]
                });

                const localPortIcon = context.containerSelection.select('#id-local-port').select('text.port-icon');
                const remotePortIcon = context.containerSelection.select('#id-remote-port').select('text.port-icon');

                const localY = parseFloat(localPortIcon.attr('y'));
                const remoteY = parseFloat(remotePortIcon.attr('y'));

                // Remote port icon should be positioned lower due to banner
                expect(remoteY).toBeGreaterThan(localY);

                cleanup();
            });
        });

        describe('EXIT phase - removing ports', () => {
            it('should remove ports that are no longer in data', async () => {
                const port1 = createMockPort({ entity: { id: 'port-1' } });
                const port2 = createMockPort({ entity: { id: 'port-2' } });

                const { context, getPortElements, cleanup } = await setup({
                    ports: [port1, port2]
                });

                expect(getPortElements().size()).toBe(2);

                // Re-render with only port1
                context.ports = [port1];
                PortRenderer.render(context);

                expect(getPortElements().size()).toBe(1);
                expect(getPortElements().attr('id')).toBe('id-port-1');

                cleanup();
            });

            it('should remove all ports when data is empty', async () => {
                const port = createMockPort();
                const { context, getPortElements, cleanup } = await setup({
                    ports: [port]
                });

                expect(getPortElements().size()).toBe(1);

                // Re-render with empty array
                context.ports = [];
                PortRenderer.render(context);

                expect(getPortElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const port = createMockPort();
            const { getPortElements, cleanup } = await setup({
                ports: [port],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const portEl = getPortElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            portEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(port, expect.any(MouseEvent));

            cleanup();
        });

        it('should not attach click handler when canSelect is false', async () => {
            const onClick = vi.fn();
            const port = createMockPort();
            const { getPortElements, cleanup } = await setup({
                ports: [port],
                canSelect: false,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const portEl = getPortElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            portEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should ignore right-click events', async () => {
            const onClick = vi.fn();
            const port = createMockPort();
            const { getPortElements, cleanup } = await setup({
                ports: [port],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate right-click (button: 2)
            const portEl = getPortElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            portEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should attach double-click handler when onDoubleClick callback provided', async () => {
            const onDoubleClick = vi.fn();
            const port = createMockPort();
            const { getPortElements, cleanup } = await setup({
                ports: [port],
                canSelect: true,
                callbacks: { onDoubleClick }
            });

            // Simulate dblclick event
            const portEl = getPortElements().node()!;
            const event = new MouseEvent('dblclick', { bubbles: true });
            portEl.dispatchEvent(event);

            expect(onDoubleClick).toHaveBeenCalledTimes(1);
            expect(onDoubleClick).toHaveBeenCalledWith(port, expect.any(MouseEvent));

            cleanup();
        });
    });

    describe('pan', () => {
        it('should update port elements for entering/leaving ports', async () => {
            const port = createMockPort({
                entity: { position: { x: 500, y: 600 } }
            });
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            // Mark as entering (becoming visible)
            getPortElements().classed('entering', true).classed('visible', true);

            // Call pan to update
            PortRenderer.pan(getPortElements(), context);

            // Should have created details since it's now visible
            expect(getPortElements().select('g.port-details').empty()).toBe(false);

            cleanup();
        });

        it('should remove details for leaving ports', async () => {
            const port = createMockPort();
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            // First make visible
            getPortElements().classed('visible', true);
            PortRenderer.render(context);
            expect(getPortElements().select('g.port-details').empty()).toBe(false);

            // Now mark as leaving (becoming not visible)
            getPortElements().classed('visible', false).classed('leaving', true);

            // Call pan to update
            PortRenderer.pan(getPortElements(), context);

            // Details should be removed
            expect(getPortElements().select('g.port-details').empty()).toBe(true);

            cleanup();
        });
    });

    describe('componentUtils integration', () => {
        it('should call bulletins utility', async () => {
            const port = createMockPort({
                entity: {
                    bulletins: [{ id: 1, message: 'Test bulletin' }]
                }
            });
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            getPortElements().classed('visible', true);
            PortRenderer.render(context);

            expect(context.componentUtils.bulletins).toHaveBeenCalled();

            cleanup();
        });

        it('should call activeThreadCount utility', async () => {
            const port = createMockPort();
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            getPortElements().classed('visible', true);
            PortRenderer.render(context);

            expect(context.componentUtils.activeThreadCount).toHaveBeenCalled();

            cleanup();
        });

        it('should call comments utility when port has read permission', async () => {
            const port = createMockPort({
                entity: {
                    component: { comments: 'Test comment' }
                }
            });
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            getPortElements().classed('visible', true);
            PortRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), 'Test comment');

            cleanup();
        });

        it('should call comments utility with null when port has no read permission', async () => {
            const port = createMockPort({
                entity: {
                    permissions: { canRead: false, canWrite: false },
                    component: { comments: 'Test comment' }
                }
            });
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            getPortElements().classed('visible', true);
            PortRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), null);

            cleanup();
        });
    });

    describe('port name handling', () => {
        it('should show abbreviated name when port is not visible', async () => {
            const port = createMockPort({
                entity: {
                    component: { name: 'This is a very long port name that should be truncated' }
                }
            });
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            // Make sure port is NOT visible
            getPortElements().classed('visible', false);
            PortRenderer.render(context);

            const portName = getPortElements().select('text.port-name');
            // Should be truncated to 15 chars + ellipsis
            expect(portName.text()).toBe('This is a very …');

            cleanup();
        });

        it('should clear port name when port is not visible and has no read permission', async () => {
            const port = createMockPort({
                entity: {
                    permissions: { canRead: false, canWrite: false },
                    component: { name: 'Test Port' }
                }
            });
            const { context, getPortElements, cleanup } = await setup({
                ports: [port]
            });

            // Make sure port is NOT visible
            getPortElements().classed('visible', false);
            PortRenderer.render(context);

            const portName = getPortElements().select('text.port-name');
            expect(portName.text()).toBe('');

            cleanup();
        });
    });
});
