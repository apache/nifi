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
import { RemoteProcessGroupRenderer } from './remote-process-group-renderer';
import { RemoteProcessGroupRenderContext } from '../render-context.types';
import { CanvasRemoteProcessGroup } from '../../canvas.types';
import { ComponentType } from '@nifi/shared';

/**
 * Test setup options for RemoteProcessGroupRenderer tests
 */
interface SetupOptions {
    remoteProcessGroups?: CanvasRemoteProcessGroup[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledRemoteProcessGroupIds?: Set<string>;
    callbacks?: Partial<RemoteProcessGroupRenderContext['callbacks']>;
}

/**
 * Creates a mock remote process group entity for testing
 */
function createMockRemoteProcessGroup(
    overrides: {
        entity?: {
            id?: string;
            position?: { x: number; y: number };
            permissions?: { canRead?: boolean; canWrite?: boolean };
            component?: {
                name?: string;
                comments?: string;
                targetUri?: string;
                targetUris?: string;
                targetSecure?: boolean;
                flowRefreshed?: string;
                validationErrors?: string[];
                authorizationIssues?: string[];
            };
            status?: {
                transmissionStatus?: string;
                aggregateSnapshot?: {
                    sent?: string;
                    received?: string;
                };
            };
            bulletins?: any[];
            inputPortCount?: number;
            outputPortCount?: number;
        };
        ui?: {
            dimensions?: { width: number; height: number };
            currentPosition?: { x: number; y: number };
        };
    } = {}
): CanvasRemoteProcessGroup {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    const defaultRpg: CanvasRemoteProcessGroup = {
        entity: {
            id: entityOverrides.id ?? 'rpg-1',
            position: entityOverrides.position ?? { x: 100, y: 200 },
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            },
            component: {
                name: entityOverrides.component?.name ?? 'Test Remote Process Group',
                comments: entityOverrides.component?.comments ?? '',
                targetUri: entityOverrides.component?.targetUri ?? 'https://remote-nifi:8443/nifi',
                targetUris: entityOverrides.component?.targetUris,
                targetSecure: entityOverrides.component?.targetSecure ?? true,
                flowRefreshed: entityOverrides.component?.flowRefreshed ?? '01/15/2024 10:30:00 UTC',
                validationErrors: entityOverrides.component?.validationErrors ?? [],
                authorizationIssues: entityOverrides.component?.authorizationIssues ?? []
            },
            status: {
                transmissionStatus: entityOverrides.status?.transmissionStatus ?? 'Stopped',
                aggregateSnapshot: {
                    sent: entityOverrides.status?.aggregateSnapshot?.sent ?? '0 (0 bytes)',
                    received: entityOverrides.status?.aggregateSnapshot?.received ?? '0 (0 bytes)'
                }
            },
            bulletins: entityOverrides.bulletins ?? [],
            inputPortCount: entityOverrides.inputPortCount ?? 0,
            outputPortCount: entityOverrides.outputPortCount ?? 0
        },
        ui: {
            componentType: ComponentType.RemoteProcessGroup,
            dimensions: uiOverrides.dimensions ?? { width: 384, height: 176 },
            currentPosition: uiOverrides.currentPosition
        }
    };

    return defaultRpg;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): RemoteProcessGroupRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('rpg-container');
    document.body.appendChild(container);

    return {
        containerSelection: d3.select(container),
        remoteProcessGroups: options.remoteProcessGroups || [],
        canSelect: options.canSelect ?? true,
        disabledRemoteProcessGroupIds: options.disabledRemoteProcessGroupIds,
        getCanEdit: () => options.canEdit ?? true,
        textEllipsis: {
            applyEllipsis: vi.fn((selection, text, _className) => {
                selection.text(text);
            })
        } as any,
        formatUtils: {
            formatQueuedStats: vi.fn((str) => {
                const match = str.match(/^(\d+)\s*\((.+)\)$/);
                if (match) {
                    return { count: match[1], size: ` (${match[2]})` };
                }
                return { count: str, size: '' };
            })
        } as any,
        nifiCommon: {} as any,
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

    // Render remote process groups
    RemoteProcessGroupRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getRpgElements: () =>
            context.containerSelection.selectAll<SVGGElement, CanvasRemoteProcessGroup>('g.remote-process-group'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.rpg-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('RemoteProcessGroupRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.rpg-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new remote process groups', () => {
            it('should create remote process group elements', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const elements = getRpgElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-rpg-1');
                expect(elements.classed('remote-process-group')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create border and body rectangles', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const rpgEl = getRpgElements();
                expect(rpgEl.select('rect.border').empty()).toBe(false);
                expect(rpgEl.select('rect.body').empty()).toBe(false);

                cleanup();
            });

            it('should create remote process group banner', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const banner = getRpgElements().select('rect.remote-process-group-banner');
                expect(banner.empty()).toBe(false);

                cleanup();
            });

            it('should create remote process group name element', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: { component: { name: 'My Remote PG' } }
                });
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const rpgName = getRpgElements().select('text.remote-process-group-name');
                expect(rpgName.empty()).toBe(false);
                expect(rpgName.text()).toBe('My Remote PG');

                cleanup();
            });

            it('should create multiple remote process groups', async () => {
                const rpg1 = createMockRemoteProcessGroup({ entity: { id: 'rpg-1' } });
                const rpg2 = createMockRemoteProcessGroup({ entity: { id: 'rpg-2' } });
                const rpg3 = createMockRemoteProcessGroup({ entity: { id: 'rpg-3' } });

                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg1, rpg2, rpg3]
                });

                expect(getRpgElements().size()).toBe(3);

                cleanup();
            });
        });

        describe('UPDATE phase - positioning and styling', () => {
            it('should position remote process groups using transform', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: { position: { x: 150, y: 250 } }
                });
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const transform = getRpgElements().attr('transform');
                expect(transform).toBe('translate(150, 250)');

                cleanup();
            });

            it('should use ui.currentPosition when set (during drag)', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: { position: { x: 100, y: 100 } },
                    ui: {
                        dimensions: { width: 384, height: 176 },
                        currentPosition: { x: 200, y: 300 }
                    }
                });
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const transform = getRpgElements().attr('transform');
                expect(transform).toBe('translate(200, 300)');

                cleanup();
            });

            it('should apply disabled styling when rpg is in disabledRemoteProcessGroupIds', async () => {
                const rpg = createMockRemoteProcessGroup({ entity: { id: 'rpg-disabled' } });
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg],
                    disabledRemoteProcessGroupIds: new Set(['rpg-disabled'])
                });

                const rpgEl = getRpgElements();
                expect(rpgEl.classed('disabled')).toBe(true);
                expect(rpgEl.style('opacity')).toBe('0.6');
                expect(rpgEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg],
                    canSelect: true
                });

                expect(getRpgElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg],
                    canSelect: false
                });

                expect(getRpgElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                const rpgEl = getRpgElements();
                expect(rpgEl.select('rect.border').classed('unauthorized')).toBe(true);
                expect(rpgEl.select('rect.body').classed('unauthorized')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - visible remote process groups with details', () => {
            it('should create details group when rpg is visible', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                // Mark rpg as visible
                getRpgElements().classed('visible', true);

                // Re-render to trigger details creation
                RemoteProcessGroupRenderer.render(context);

                expect(getRpgElements().select('g.remote-process-group-details').empty()).toBe(false);

                cleanup();
            });

            it('should create transmission status container in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('g.remote-process-group-transmission-status-container').empty()).toBe(false);
                expect(details.select('rect.remote-process-group-transmission-status-background').empty()).toBe(false);
                expect(details.select('text.remote-process-group-transmission-status').empty()).toBe(false);

                cleanup();
            });

            it('should create URI section in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('rect.remote-process-group-details-banner').empty()).toBe(false);
                expect(details.select('text.remote-process-group-transmission-secure').empty()).toBe(false);
                expect(details.select('text.remote-process-group-uri').empty()).toBe(false);

                cleanup();
            });

            it('should create statistics rows in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('rect.remote-process-group-sent-stats').empty()).toBe(false);
                expect(details.select('rect.remote-process-group-received-stats').empty()).toBe(false);
                expect(details.select('text.remote-process-group-sent').empty()).toBe(false);
                expect(details.select('text.remote-process-group-received').empty()).toBe(false);

                cleanup();
            });

            it('should create last refresh banner in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('rect.remote-process-group-last-refresh-rect').empty()).toBe(false);
                expect(details.select('text.remote-process-group-last-refresh').empty()).toBe(false);

                cleanup();
            });

            it('should create bulletin elements in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('rect.bulletin-background').empty()).toBe(false);
                expect(details.select('text.bulletin-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create active thread count elements in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('text.active-thread-count-icon').empty()).toBe(false);
                expect(details.select('text.active-thread-count').empty()).toBe(false);

                cleanup();
            });

            it('should create comment icon in details', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const details = getRpgElements().select('g.remote-process-group-details');
                expect(details.select('text.component-comments').empty()).toBe(false);

                cleanup();
            });

            it('should remove details when rpg becomes not visible', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                // First make visible and render details
                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);
                expect(getRpgElements().select('g.remote-process-group-details').empty()).toBe(false);

                // Now make not visible and re-render
                getRpgElements().classed('visible', false);
                RemoteProcessGroupRenderer.render(context);
                expect(getRpgElements().select('g.remote-process-group-details').empty()).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - transmission status', () => {
            it('should show transmitting icon when transmitting', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        status: { transmissionStatus: 'Transmitting' }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const transmissionStatus = getRpgElements().select('text.remote-process-group-transmission-status');
                expect(transmissionStatus.text()).toBe('\uf140'); // Transmitting icon
                expect(transmissionStatus.classed('transmitting')).toBe(true);

                cleanup();
            });

            it('should show not-transmitting icon when not transmitting', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        status: { transmissionStatus: 'Stopped' }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const transmissionStatus = getRpgElements().select('text.remote-process-group-transmission-status');
                expect(transmissionStatus.text()).toBe('\ue80a'); // Not transmitting icon (flowfont)
                expect(transmissionStatus.classed('not-transmitting')).toBe(true);

                cleanup();
            });

            it('should show warning icon when there are validation errors', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        component: {
                            validationErrors: ['Connection error']
                        }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const transmissionStatus = getRpgElements().select('text.remote-process-group-transmission-status');
                expect(transmissionStatus.text()).toBe('\uf071'); // Warning icon
                expect(transmissionStatus.classed('invalid')).toBe(true);
                expect(transmissionStatus.classed('caution')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - secure transfer indicator', () => {
            it('should show lock icon when target is secure', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        component: { targetSecure: true }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const secureIcon = getRpgElements().select('text.remote-process-group-transmission-secure');
                expect(secureIcon.text()).toBe('\uf023'); // Lock icon
                expect(secureIcon.classed('success-color')).toBe(true);

                cleanup();
            });

            it('should show globe icon when target is not secure', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        component: { targetSecure: false }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const secureIcon = getRpgElements().select('text.remote-process-group-transmission-secure');
                expect(secureIcon.text()).toBe('\uf09c'); // Globe/unlock icon
                expect(secureIcon.classed('success-color')).toBe(false);

                cleanup();
            });
        });

        describe('UPDATE phase - statistics display', () => {
            it('should display sent statistics', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        status: {
                            aggregateSnapshot: {
                                sent: '100 (1.5 MB)'
                            }
                        }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                expect(context.formatUtils.formatQueuedStats).toHaveBeenCalledWith('100 (1.5 MB)');

                cleanup();
            });

            it('should display received statistics', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        status: {
                            aggregateSnapshot: {
                                received: '50 (500 KB)'
                            }
                        }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                expect(context.formatUtils.formatQueuedStats).toHaveBeenCalledWith('50 (500 KB)');

                cleanup();
            });

            it('should display input port count in sent stats', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        inputPortCount: 3
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const sentPorts = getRpgElements().select('text.remote-process-group-sent tspan.ports');
                expect(sentPorts.text()).toContain('3');

                cleanup();
            });

            it('should display output port count in received stats', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        outputPortCount: 2
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const receivedPorts = getRpgElements().select('text.remote-process-group-received tspan.ports');
                expect(receivedPorts.text()).toContain('2');

                cleanup();
            });
        });

        describe('UPDATE phase - last refresh display', () => {
            it('should display flow refreshed time', async () => {
                const rpg = createMockRemoteProcessGroup({
                    entity: {
                        component: { flowRefreshed: '01/20/2024 14:30:00 UTC' }
                    }
                });
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const lastRefresh = getRpgElements().select('text.remote-process-group-last-refresh');
                expect(lastRefresh.text()).toBe('01/20/2024 14:30:00 UTC');

                cleanup();
            });

            it('should display "Remote flow not current" when flowRefreshed is not set', async () => {
                const rpg = createMockRemoteProcessGroup();
                // Explicitly set flowRefreshed to undefined to test the fallback
                (rpg.entity.component as any).flowRefreshed = undefined;
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                getRpgElements().classed('visible', true);
                RemoteProcessGroupRenderer.render(context);

                const lastRefresh = getRpgElements().select('text.remote-process-group-last-refresh');
                expect(lastRefresh.text()).toBe('Remote flow not current');

                cleanup();
            });
        });

        describe('EXIT phase - removing remote process groups', () => {
            it('should remove rpgs that are no longer in data', async () => {
                const rpg1 = createMockRemoteProcessGroup({ entity: { id: 'rpg-1' } });
                const rpg2 = createMockRemoteProcessGroup({ entity: { id: 'rpg-2' } });

                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg1, rpg2]
                });

                expect(getRpgElements().size()).toBe(2);

                // Re-render with only rpg1
                context.remoteProcessGroups = [rpg1];
                RemoteProcessGroupRenderer.render(context);

                expect(getRpgElements().size()).toBe(1);
                expect(getRpgElements().attr('id')).toBe('id-rpg-1');

                cleanup();
            });

            it('should remove all rpgs when data is empty', async () => {
                const rpg = createMockRemoteProcessGroup();
                const { context, getRpgElements, cleanup } = await setup({
                    remoteProcessGroups: [rpg]
                });

                expect(getRpgElements().size()).toBe(1);

                // Re-render with empty array
                context.remoteProcessGroups = [];
                RemoteProcessGroupRenderer.render(context);

                expect(getRpgElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const rpg = createMockRemoteProcessGroup();
            const { getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const rpgEl = getRpgElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            rpgEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(rpg, expect.any(MouseEvent));

            cleanup();
        });

        it('should not attach click handler when canSelect is false', async () => {
            const onClick = vi.fn();
            const rpg = createMockRemoteProcessGroup();
            const { getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg],
                canSelect: false,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const rpgEl = getRpgElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            rpgEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should ignore right-click events', async () => {
            const onClick = vi.fn();
            const rpg = createMockRemoteProcessGroup();
            const { getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate right-click (button: 2)
            const rpgEl = getRpgElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            rpgEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should attach double-click handler when onDoubleClick callback provided', async () => {
            const onDoubleClick = vi.fn();
            const rpg = createMockRemoteProcessGroup();
            const { getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg],
                canSelect: true,
                callbacks: { onDoubleClick }
            });

            // Simulate dblclick event
            const rpgEl = getRpgElements().node()!;
            const event = new MouseEvent('dblclick', { bubbles: true });
            rpgEl.dispatchEvent(event);

            expect(onDoubleClick).toHaveBeenCalledTimes(1);
            expect(onDoubleClick).toHaveBeenCalledWith(rpg, expect.any(MouseEvent));

            cleanup();
        });
    });

    describe('pan', () => {
        it('should update rpg elements for entering/leaving rpgs', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: { position: { x: 500, y: 600 } }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            // Mark as entering (becoming visible)
            getRpgElements().classed('entering', true).classed('visible', true);

            // Call pan to update
            RemoteProcessGroupRenderer.pan(getRpgElements(), context);

            // Should have created details since it's now visible
            expect(getRpgElements().select('g.remote-process-group-details').empty()).toBe(false);

            cleanup();
        });

        it('should remove details for leaving rpgs', async () => {
            const rpg = createMockRemoteProcessGroup();
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            // First make visible
            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);
            expect(getRpgElements().select('g.remote-process-group-details').empty()).toBe(false);

            // Now mark as leaving (becoming not visible)
            getRpgElements().classed('visible', false).classed('leaving', true);

            // Call pan to update
            RemoteProcessGroupRenderer.pan(getRpgElements(), context);

            // Details should be removed
            expect(getRpgElements().select('g.remote-process-group-details').empty()).toBe(true);

            cleanup();
        });
    });

    describe('componentUtils integration', () => {
        it('should call bulletins utility', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    bulletins: [{ id: 1, message: 'Test bulletin' }]
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.componentUtils.bulletins).toHaveBeenCalled();

            cleanup();
        });

        it('should call activeThreadCount utility', async () => {
            const rpg = createMockRemoteProcessGroup();
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.componentUtils.activeThreadCount).toHaveBeenCalled();

            cleanup();
        });

        it('should call comments utility when rpg has read permission', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    component: { comments: 'Test comment' }
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), 'Test comment');

            cleanup();
        });

        it('should call comments utility with null when rpg has no read permission', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    permissions: { canRead: false, canWrite: false },
                    component: { comments: 'Test comment' }
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), null);

            cleanup();
        });

        it('should call canvasTooltip when there are validation errors', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    component: {
                        validationErrors: ['Connection error']
                    }
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.componentUtils.canvasTooltip).toHaveBeenCalled();

            cleanup();
        });

        it('should call resetCanvasTooltip when there are no validation errors', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    component: {
                        validationErrors: []
                    }
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.componentUtils.resetCanvasTooltip).toHaveBeenCalled();

            cleanup();
        });
    });

    describe('URI display', () => {
        it('should display target URI', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    component: { targetUri: 'https://remote-nifi:8443/nifi' }
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            expect(context.textEllipsis.applyEllipsis).toHaveBeenCalledWith(
                expect.anything(),
                'https://remote-nifi:8443/nifi',
                'rpg-uri'
            );

            cleanup();
        });

        it('should clear URI when no read permission', async () => {
            const rpg = createMockRemoteProcessGroup({
                entity: {
                    permissions: { canRead: false, canWrite: false }
                }
            });
            const { context, getRpgElements, cleanup } = await setup({
                remoteProcessGroups: [rpg]
            });

            getRpgElements().classed('visible', true);
            RemoteProcessGroupRenderer.render(context);

            const uriText = getRpgElements().select('text.remote-process-group-uri');
            expect(uriText.text()).toBe('');

            cleanup();
        });
    });
});
