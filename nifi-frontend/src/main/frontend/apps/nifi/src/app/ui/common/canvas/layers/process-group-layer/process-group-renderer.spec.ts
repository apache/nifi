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
import { ProcessGroupRenderer } from './process-group-renderer';
import { ProcessGroupRenderContext } from '../render-context.types';
import { createBaseRenderContextFixture } from '../render-context-fixtures';
import { RegistryClientEntity } from '../../../../state/shared';
import { CanvasProcessGroup } from '../../canvas.types';
import { BulletinEntity, ComponentType } from '@nifi/shared';

/**
 * Test setup options for ProcessGroupRenderer tests
 */
interface SetupOptions {
    processGroups?: CanvasProcessGroup[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledProcessGroupIds?: Set<string>;
    registryClients?: RegistryClientEntity[];
    callbacks?: Partial<ProcessGroupRenderContext['callbacks']>;
}

/**
 * Creates a mock process group entity for testing
 */
function createMockProcessGroup(
    overrides: {
        entity?: {
            id?: string;
            position?: { x: number; y: number };
            permissions?: { canRead?: boolean; canWrite?: boolean };
            versionedFlowState?: string | null;
            component?: {
                name?: string;
                comments?: string;
                versionControlInformation?: NonNullable<
                    CanvasProcessGroup['entity']['component']
                >['versionControlInformation'];
            };
            status?: {
                aggregateSnapshot?: {
                    queued?: string;
                    input?: string;
                    read?: string;
                    written?: string;
                    output?: string;
                };
            };
            bulletins?: BulletinEntity[];
            // Component counts
            inputPortCount?: number;
            outputPortCount?: number;
            activeRemotePortCount?: number;
            inactiveRemotePortCount?: number;
            runningCount?: number;
            stoppedCount?: number;
            invalidCount?: number;
            disabledCount?: number;
            // Version control counts
            upToDateCount?: number;
            locallyModifiedCount?: number;
            staleCount?: number;
            locallyModifiedAndStaleCount?: number;
            syncFailureCount?: number;
        };
        ui?: {
            dimensions?: { width: number; height: number };
            currentPosition?: { x: number; y: number };
        };
    } = {}
): CanvasProcessGroup {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    const defaultProcessGroup: CanvasProcessGroup = {
        entity: {
            id: entityOverrides.id ?? 'pg-1',
            position: entityOverrides.position ?? { x: 100, y: 200 },
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            },
            versionedFlowState: entityOverrides.versionedFlowState ?? null,
            component: {
                name: entityOverrides.component?.name ?? 'Test Process Group',
                comments: entityOverrides.component?.comments ?? '',
                versionControlInformation: entityOverrides.component?.versionControlInformation ?? null
            },
            status: {
                aggregateSnapshot: {
                    queued: entityOverrides.status?.aggregateSnapshot?.queued ?? '0 (0 bytes)',
                    input: entityOverrides.status?.aggregateSnapshot?.input ?? '0 (0 bytes)',
                    read: entityOverrides.status?.aggregateSnapshot?.read ?? '0 bytes',
                    written: entityOverrides.status?.aggregateSnapshot?.written ?? '0 bytes',
                    output: entityOverrides.status?.aggregateSnapshot?.output ?? '0 (0 bytes)'
                }
            },
            bulletins: entityOverrides.bulletins ?? [],
            // Component counts
            inputPortCount: entityOverrides.inputPortCount ?? 0,
            outputPortCount: entityOverrides.outputPortCount ?? 0,
            activeRemotePortCount: entityOverrides.activeRemotePortCount ?? 0,
            inactiveRemotePortCount: entityOverrides.inactiveRemotePortCount ?? 0,
            runningCount: entityOverrides.runningCount ?? 0,
            stoppedCount: entityOverrides.stoppedCount ?? 0,
            invalidCount: entityOverrides.invalidCount ?? 0,
            disabledCount: entityOverrides.disabledCount ?? 0,
            // Version control counts
            upToDateCount: entityOverrides.upToDateCount ?? 0,
            locallyModifiedCount: entityOverrides.locallyModifiedCount ?? 0,
            staleCount: entityOverrides.staleCount ?? 0,
            locallyModifiedAndStaleCount: entityOverrides.locallyModifiedAndStaleCount ?? 0,
            syncFailureCount: entityOverrides.syncFailureCount ?? 0
        },
        ui: {
            componentType: ComponentType.ProcessGroup,
            dimensions: uiOverrides.dimensions ?? { width: 384, height: 176 },
            currentPosition: uiOverrides.currentPosition
        }
    };

    return defaultProcessGroup;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): ProcessGroupRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('process-group-container');
    document.body.appendChild(container);

    return {
        containerSelection: d3.select(container),
        processGroups: options.processGroups || [],
        canSelect: options.canSelect ?? true,
        disabledProcessGroupIds: options.disabledProcessGroupIds,
        getDisabledProcessGroupIds: () => options.disabledProcessGroupIds ?? new Set(),
        registryClients: options.registryClients || [],
        getCanEdit: () => options.canEdit ?? true,
        getCanSelect: () => options.canSelect ?? true,
        getSelectedIds: () => new Set(),
        canvasRootResolver: () => d3.select(container),
        ...createBaseRenderContextFixture({
            textEllipsis: {
                applyEllipsis: vi.fn((selection, text, _className) => {
                    selection.text(text);
                })
            },
            formatUtils: {},
            nifiCommon: {}
        }),
        componentUtils: {
            bulletins: vi.fn(),
            activeThreadCount: vi.fn(),
            comments: vi.fn(),
            canvasTooltip: vi.fn(),
            resetCanvasTooltip: vi.fn()
        } as unknown as ProcessGroupRenderContext['componentUtils'],
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

    // Render process groups
    ProcessGroupRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getProcessGroupElements: () =>
            context.containerSelection.selectAll<SVGGElement, CanvasProcessGroup>('g.process-group'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.process-group-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('ProcessGroupRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.process-group-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new process groups', () => {
            it('should create process group elements', async () => {
                const pg = createMockProcessGroup();
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const elements = getProcessGroupElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-pg-1');
                expect(elements.classed('process-group')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create border and body rectangles', async () => {
                const pg = createMockProcessGroup();
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const pgEl = getProcessGroupElements();
                expect(pgEl.select('rect.border').empty()).toBe(false);
                expect(pgEl.select('rect.body').empty()).toBe(false);

                cleanup();
            });

            it('should create process group banner', async () => {
                const pg = createMockProcessGroup();
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const banner = getProcessGroupElements().select('rect.process-group-banner');
                expect(banner.empty()).toBe(false);

                cleanup();
            });

            it('should create process group name element', async () => {
                const pg = createMockProcessGroup({
                    entity: { component: { name: 'My Process Group' } }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const pgName = getProcessGroupElements().select('text.process-group-name');
                expect(pgName.empty()).toBe(false);
                // Check the first text node directly (the <title> child is excluded)
                expect((pgName.node() as Element).childNodes[0]?.textContent).toBe('My Process Group');

                cleanup();
            });

            it('should create version control container', async () => {
                const pg = createMockProcessGroup();
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControlContainer = getProcessGroupElements().select('g.version-control-container');
                expect(versionControlContainer.empty()).toBe(false);
                expect(versionControlContainer.select('rect.version-control-background').empty()).toBe(false);
                expect(versionControlContainer.select('rect.version-control-background-fill').empty()).toBe(false);
                expect(versionControlContainer.select('text.version-control').empty()).toBe(false);

                cleanup();
            });

            it('should create multiple process groups', async () => {
                const pg1 = createMockProcessGroup({ entity: { id: 'pg-1' } });
                const pg2 = createMockProcessGroup({ entity: { id: 'pg-2' } });
                const pg3 = createMockProcessGroup({ entity: { id: 'pg-3' } });

                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg1, pg2, pg3]
                });

                expect(getProcessGroupElements().size()).toBe(3);

                cleanup();
            });
        });

        describe('UPDATE phase - positioning and styling', () => {
            it('should position process groups using transform', async () => {
                const pg = createMockProcessGroup({
                    entity: { position: { x: 150, y: 250 } }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const transform = getProcessGroupElements().attr('transform');
                expect(transform).toBe('translate(150, 250)');

                cleanup();
            });

            it('should use ui.currentPosition when set (during drag)', async () => {
                const pg = createMockProcessGroup({
                    entity: { position: { x: 100, y: 100 } },
                    ui: {
                        dimensions: { width: 384, height: 176 },
                        currentPosition: { x: 200, y: 300 }
                    }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const transform = getProcessGroupElements().attr('transform');
                expect(transform).toBe('translate(200, 300)');

                cleanup();
            });

            it('should apply disabled styling when process group is in disabledProcessGroupIds', async () => {
                const pg = createMockProcessGroup({ entity: { id: 'pg-disabled' } });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg],
                    disabledProcessGroupIds: new Set(['pg-disabled'])
                });

                const pgEl = getProcessGroupElements();
                expect(pgEl.classed('disabled')).toBe(true);
                expect(pgEl.style('opacity')).toBe('0.6');
                expect(pgEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const pg = createMockProcessGroup();
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg],
                    canSelect: true
                });

                expect(getProcessGroupElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const pg = createMockProcessGroup();
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg],
                    canSelect: false
                });

                expect(getProcessGroupElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const pg = createMockProcessGroup({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const pgEl = getProcessGroupElements();
                expect(pgEl.select('rect.border').classed('unauthorized')).toBe(true);
                expect(pgEl.select('rect.body').classed('unauthorized')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - version control status', () => {
            it('should show UP_TO_DATE version control icon', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: 'UP_TO_DATE' }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControl = getProcessGroupElements().select('text.version-control');
                expect(versionControl.text()).toBe('\uf00c'); // Check mark icon
                expect(versionControl.classed('success')).toBe(true);

                cleanup();
            });

            it('should show LOCALLY_MODIFIED version control icon', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: 'LOCALLY_MODIFIED' }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControl = getProcessGroupElements().select('text.version-control');
                expect(versionControl.text()).toBe('\uf069'); // Asterisk icon
                expect(versionControl.classed('info')).toBe(true);

                cleanup();
            });

            it('should show STALE version control icon', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: 'STALE' }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControl = getProcessGroupElements().select('text.version-control');
                expect(versionControl.text()).toBe('\uf0aa'); // Arrow up icon
                expect(versionControl.classed('critical')).toBe(true);

                cleanup();
            });

            it('should show LOCALLY_MODIFIED_AND_STALE version control icon', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: 'LOCALLY_MODIFIED_AND_STALE' }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControl = getProcessGroupElements().select('text.version-control');
                expect(versionControl.text()).toBe('\uf06a'); // Exclamation circle icon
                expect(versionControl.classed('critical')).toBe(true);

                cleanup();
            });

            it('should show SYNC_FAILURE version control icon', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: 'SYNC_FAILURE' }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControl = getProcessGroupElements().select('text.version-control');
                expect(versionControl.text()).toBe('\uf128'); // Question mark icon
                expect(versionControl.classed('info')).toBe(true);

                cleanup();
            });

            it('should hide version control background when not under version control', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: null }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControlBackgroundFill = getProcessGroupElements().select(
                    'rect.version-control-background-fill'
                );
                expect(versionControlBackgroundFill.style('visibility')).toBe('hidden');

                cleanup();
            });

            it('should show version control background when under version control', async () => {
                const pg = createMockProcessGroup({
                    entity: { versionedFlowState: 'UP_TO_DATE' }
                });
                const { getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                const versionControlBackgroundFill = getProcessGroupElements().select(
                    'rect.version-control-background-fill'
                );
                expect(versionControlBackgroundFill.style('visibility')).toBe('visible');

                cleanup();
            });
        });

        describe('UPDATE phase - visible process groups with details', () => {
            it('should create details group when process group is visible', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                // Mark process group as visible
                getProcessGroupElements().classed('visible', true);

                // Re-render to trigger details creation
                ProcessGroupRenderer.render(context);

                expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(false);

                cleanup();
            });

            it('should create component status icons in details', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const details = getProcessGroupElements().select('g.process-group-details');
                expect(details.select('text.process-group-transmitting').empty()).toBe(false);
                expect(details.select('text.process-group-not-transmitting').empty()).toBe(false);
                expect(details.select('text.process-group-running').empty()).toBe(false);
                expect(details.select('text.process-group-stopped').empty()).toBe(false);
                expect(details.select('text.process-group-invalid').empty()).toBe(false);
                expect(details.select('text.process-group-disabled').empty()).toBe(false);

                cleanup();
            });

            it('should create statistics rows in details', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const details = getProcessGroupElements().select('g.process-group-details');
                expect(details.select('rect.process-group-queued-stats').empty()).toBe(false);
                expect(details.select('rect.process-group-stats-in-out').empty()).toBe(false);
                expect(details.select('rect.process-group-read-write-stats').empty()).toBe(false);
                expect(details.select('text.process-group-queued').empty()).toBe(false);
                expect(details.select('text.process-group-in').empty()).toBe(false);
                expect(details.select('text.process-group-read-write').empty()).toBe(false);
                expect(details.select('text.process-group-out').empty()).toBe(false);

                cleanup();
            });

            it('should create version control status icons in details', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const details = getProcessGroupElements().select('g.process-group-details');
                expect(details.select('text.process-group-up-to-date').empty()).toBe(false);
                expect(details.select('text.process-group-locally-modified').empty()).toBe(false);
                expect(details.select('text.process-group-stale').empty()).toBe(false);
                expect(details.select('text.process-group-locally-modified-and-stale').empty()).toBe(false);
                expect(details.select('text.process-group-sync-failure').empty()).toBe(false);

                cleanup();
            });

            it('should create bulletin elements in details', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const details = getProcessGroupElements().select('g.process-group-details');
                expect(details.select('rect.bulletin-background').empty()).toBe(false);
                expect(details.select('text.bulletin-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create active thread count elements in details', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const details = getProcessGroupElements().select('g.process-group-details');
                expect(details.select('text.active-thread-count-icon').empty()).toBe(false);
                expect(details.select('text.active-thread-count').empty()).toBe(false);

                cleanup();
            });

            it('should create comment icon in details', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const details = getProcessGroupElements().select('g.process-group-details');
                expect(details.select('text.component-comments').empty()).toBe(false);

                cleanup();
            });

            it('should remove details when process group scrolls out of viewport', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                // Process group is in-viewport — details created
                ProcessGroupRenderer.render(context);
                expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(false);

                // Process group scrolls out of viewport — details removed
                context.viewportBounds = { left: 9000, top: 9000, right: 10000, bottom: 10000 };
                ProcessGroupRenderer.render(context);
                expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - component status counts', () => {
            it('should show running count with success color when running > 0', async () => {
                const pg = createMockProcessGroup({
                    entity: { runningCount: 5 }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const runningIcon = getProcessGroupElements().select('text.process-group-running');
                expect(runningIcon.classed('success-color-default')).toBe(true);
                expect(runningIcon.classed('zero')).toBe(false);

                const runningCount = getProcessGroupElements().select('text.process-group-running-count');
                expect(runningCount.text()).toBe('5');

                cleanup();
            });

            it('should show stopped count with error color when stopped > 0', async () => {
                const pg = createMockProcessGroup({
                    entity: { stoppedCount: 3 }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const stoppedIcon = getProcessGroupElements().select('text.process-group-stopped');
                expect(stoppedIcon.classed('error-color-variant')).toBe(true);
                expect(stoppedIcon.classed('zero')).toBe(false);

                const stoppedCount = getProcessGroupElements().select('text.process-group-stopped-count');
                expect(stoppedCount.text()).toBe('3');

                cleanup();
            });

            it('should show invalid count with caution color when invalid > 0', async () => {
                const pg = createMockProcessGroup({
                    entity: { invalidCount: 2 }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const invalidIcon = getProcessGroupElements().select('text.process-group-invalid');
                expect(invalidIcon.classed('caution-color')).toBe(true);
                expect(invalidIcon.classed('zero')).toBe(false);

                const invalidCount = getProcessGroupElements().select('text.process-group-invalid-count');
                expect(invalidCount.text()).toBe('2');

                cleanup();
            });

            it('should show disabled count with neutral color when disabled > 0', async () => {
                const pg = createMockProcessGroup({
                    entity: { disabledCount: 4 }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const disabledIcon = getProcessGroupElements().select('text.process-group-disabled');
                expect(disabledIcon.classed('neutral-color')).toBe(true);
                expect(disabledIcon.classed('zero')).toBe(false);

                const disabledCount = getProcessGroupElements().select('text.process-group-disabled-count');
                expect(disabledCount.text()).toBe('4');

                cleanup();
            });

            it('should add zero class when count is 0', async () => {
                const pg = createMockProcessGroup({
                    entity: { runningCount: 0, stoppedCount: 0, invalidCount: 0, disabledCount: 0 }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                expect(getProcessGroupElements().select('text.process-group-running').classed('zero')).toBe(true);
                expect(getProcessGroupElements().select('text.process-group-stopped').classed('zero')).toBe(true);
                expect(getProcessGroupElements().select('text.process-group-invalid').classed('zero')).toBe(true);
                expect(getProcessGroupElements().select('text.process-group-disabled').classed('zero')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - statistics display', () => {
            it('should display queued statistics', async () => {
                const pg = createMockProcessGroup({
                    entity: {
                        status: {
                            aggregateSnapshot: {
                                queued: '100 (1.5 MB)'
                            }
                        }
                    }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const queuedCount = getProcessGroupElements().select('text.process-group-queued tspan.count');
                const queuedSize = getProcessGroupElements().select('text.process-group-queued tspan.size');
                expect(queuedCount.text()).toBe('100');
                expect(queuedSize.text()).toBe(' (1.5 MB)');

                cleanup();
            });

            it('should display read/write statistics', async () => {
                const pg = createMockProcessGroup({
                    entity: {
                        status: {
                            aggregateSnapshot: {
                                read: '500 KB',
                                written: '1.2 MB'
                            }
                        }
                    }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const readWrite = getProcessGroupElements().select('text.process-group-read-write');
                expect(readWrite.text()).toBe('500 KB / 1.2 MB');

                cleanup();
            });

            it('should display input port count', async () => {
                const pg = createMockProcessGroup({
                    entity: {
                        inputPortCount: 3
                    }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const inPorts = getProcessGroupElements().select('text.process-group-in tspan.ports');
                // Arrow character + port count
                expect(inPorts.text()).toContain('3');

                cleanup();
            });

            it('should display output port count', async () => {
                const pg = createMockProcessGroup({
                    entity: {
                        outputPortCount: 2
                    }
                });
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                getProcessGroupElements().classed('visible', true);
                ProcessGroupRenderer.render(context);

                const outPorts = getProcessGroupElements().select('text.process-group-out tspan.ports');
                expect(outPorts.text()).toBe('2');

                cleanup();
            });
        });

        describe('EXIT phase - removing process groups', () => {
            it('should remove process groups that are no longer in data', async () => {
                const pg1 = createMockProcessGroup({ entity: { id: 'pg-1' } });
                const pg2 = createMockProcessGroup({ entity: { id: 'pg-2' } });

                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg1, pg2]
                });

                expect(getProcessGroupElements().size()).toBe(2);

                // Re-render with only pg1
                context.processGroups = [pg1];
                ProcessGroupRenderer.render(context);

                expect(getProcessGroupElements().size()).toBe(1);
                expect(getProcessGroupElements().attr('id')).toBe('id-pg-1');

                cleanup();
            });

            it('should remove all process groups when data is empty', async () => {
                const pg = createMockProcessGroup();
                const { context, getProcessGroupElements, cleanup } = await setup({
                    processGroups: [pg]
                });

                expect(getProcessGroupElements().size()).toBe(1);

                // Re-render with empty array
                context.processGroups = [];
                ProcessGroupRenderer.render(context);

                expect(getProcessGroupElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const pg = createMockProcessGroup();
            const { getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const pgEl = getProcessGroupElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            pgEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(pg, expect.any(MouseEvent));

            cleanup();
        });

        it('should not attach click handler when canSelect is false', async () => {
            const onClick = vi.fn();
            const pg = createMockProcessGroup();
            const { getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg],
                canSelect: false,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const pgEl = getProcessGroupElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            pgEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should ignore right-click events', async () => {
            const onClick = vi.fn();
            const pg = createMockProcessGroup();
            const { getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate right-click (button: 2)
            const pgEl = getProcessGroupElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            pgEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should attach double-click handler when onDoubleClick callback provided', async () => {
            const onDoubleClick = vi.fn();
            const pg = createMockProcessGroup();
            const { getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg],
                canSelect: true,
                callbacks: { onDoubleClick }
            });

            // Simulate dblclick event
            const pgEl = getProcessGroupElements().node()!;
            const event = new MouseEvent('dblclick', { bubbles: true });
            pgEl.dispatchEvent(event);

            expect(onDoubleClick).toHaveBeenCalledTimes(1);
            expect(onDoubleClick).toHaveBeenCalledWith(pg, expect.any(MouseEvent));

            cleanup();
        });
    });

    describe('visibility', () => {
        it('should create details when process group enters the viewport', async () => {
            const pg = createMockProcessGroup();
            // Start with process group outside the viewport
            const { context, getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg]
            });
            context.viewportBounds = { left: 9000, top: 9000, right: 10000, bottom: 10000 };
            ProcessGroupRenderer.render(context);
            expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(true);

            // Expand viewport to include the process group — details should be created
            context.viewportBounds = { left: -Infinity, top: -Infinity, right: Infinity, bottom: Infinity };
            ProcessGroupRenderer.render(context);
            expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(false);

            cleanup();
        });

        it('should remove details when process group leaves the viewport', async () => {
            const pg = createMockProcessGroup();
            const { context, getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg]
            });

            // Process group is in-viewport — details created
            ProcessGroupRenderer.render(context);
            expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(false);

            // Process group scrolls out of viewport — details removed
            context.viewportBounds = { left: 9000, top: 9000, right: 10000, bottom: 10000 };
            ProcessGroupRenderer.render(context);
            expect(getProcessGroupElements().select('g.process-group-details').empty()).toBe(true);

            cleanup();
        });
    });

    describe('componentUtils integration', () => {
        it('should call bulletins utility', async () => {
            const pg = createMockProcessGroup({
                entity: {
                    bulletins: [{ id: 1, message: 'Test bulletin' }]
                }
            });
            const { context, getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg]
            });

            getProcessGroupElements().classed('visible', true);
            ProcessGroupRenderer.render(context);

            expect(context.componentUtils.bulletins).toHaveBeenCalled();

            cleanup();
        });

        it('should call activeThreadCount utility', async () => {
            const pg = createMockProcessGroup();
            const { context, getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg]
            });

            getProcessGroupElements().classed('visible', true);
            ProcessGroupRenderer.render(context);

            expect(context.componentUtils.activeThreadCount).toHaveBeenCalled();

            cleanup();
        });

        it('should call comments utility when process group has read permission', async () => {
            const pg = createMockProcessGroup({
                entity: {
                    component: { comments: 'Test comment' }
                }
            });
            const { context, getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg]
            });

            getProcessGroupElements().classed('visible', true);
            ProcessGroupRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), 'Test comment');

            cleanup();
        });

        it('should call comments utility with null when process group has no read permission', async () => {
            const pg = createMockProcessGroup({
                entity: {
                    permissions: { canRead: false, canWrite: false },
                    component: { comments: 'Test comment' }
                }
            });
            const { context, getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg]
            });

            getProcessGroupElements().classed('visible', true);
            ProcessGroupRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), null);

            cleanup();
        });

        it('should call canvasTooltip for version control when under version control', async () => {
            const pg = createMockProcessGroup({
                entity: { versionedFlowState: 'UP_TO_DATE' }
            });
            const { context, cleanup } = await setup({
                processGroups: [pg]
            });

            ProcessGroupRenderer.render(context);

            expect(context.componentUtils.canvasTooltip).toHaveBeenCalled();

            cleanup();
        });

        it('should call resetCanvasTooltip for version control when not under version control', async () => {
            const pg = createMockProcessGroup({
                entity: { versionedFlowState: null }
            });
            const { context, cleanup } = await setup({
                processGroups: [pg]
            });

            ProcessGroupRenderer.render(context);

            expect(context.componentUtils.resetCanvasTooltip).toHaveBeenCalled();

            cleanup();
        });
    });

    describe('process group name positioning', () => {
        it('should offset name when under version control', async () => {
            const pgVersioned = createMockProcessGroup({
                entity: { id: 'pg-versioned', versionedFlowState: 'UP_TO_DATE' }
            });
            const pgNotVersioned = createMockProcessGroup({
                entity: { id: 'pg-not-versioned', versionedFlowState: undefined }
            });
            const { context, cleanup } = await setup({
                processGroups: [pgVersioned, pgNotVersioned]
            });

            context.containerSelection.selectAll('g.process-group').classed('visible', true);
            ProcessGroupRenderer.render(context);

            const versionedName = context.containerSelection
                .select('#id-pg-versioned')
                .select('text.process-group-name');
            const notVersionedName = context.containerSelection
                .select('#id-pg-not-versioned')
                .select('text.process-group-name');

            const versionedX = parseFloat(versionedName.attr('x'));
            const notVersionedX = parseFloat(notVersionedName.attr('x'));

            // Versioned name should be offset more to make room for version control icon
            expect(versionedX).toBeGreaterThan(notVersionedX);

            cleanup();
        });
    });

    // Selection / dblclick handlers are attached once at element entry to keep
    // the per-element listener list stable across renders. The render-time
    // `canSelect` value MUST NOT be captured by closure — `getCanSelect()` is
    // re-read at fire-time so consumers that toggle selection mode at runtime
    // get correct behavior without re-attachment churn.
    describe('canSelect runtime gating', () => {
        function setupWithControllableCanSelect(initialCanSelect: boolean) {
            const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            container.classList.add('process-group-container');
            document.body.appendChild(container);

            let canSelectFlag = initialCanSelect;
            const onClick = vi.fn();
            const onDoubleClick = vi.fn();

            const pg = createMockProcessGroup({ entity: { id: 'pg-1' } });
            const context: ProcessGroupRenderContext = {
                containerSelection: d3.select(container),
                processGroups: [pg],
                registryClients: [],
                getCanEdit: () => true,
                getCanSelect: () => canSelectFlag,
                getDisabledProcessGroupIds: () => new Set(),
                textEllipsis: { applyEllipsis: vi.fn((sel, text) => sel.text(text)) } as any,
                formatUtils: {} as any,
                nifiCommon: {} as any,
                componentUtils: {
                    bulletins: vi.fn(),
                    activeThreadCount: vi.fn(),
                    comments: vi.fn(),
                    canvasTooltip: vi.fn(),
                    resetCanvasTooltip: vi.fn()
                } as any,
                callbacks: { onClick, onDoubleClick }
            };

            ProcessGroupRenderer.render(context);
            const element = container.querySelector('g.process-group') as SVGGElement;

            return {
                element,
                onClick,
                onDoubleClick,
                setCanSelect: (v: boolean) => {
                    canSelectFlag = v;
                },
                cleanup: () => container.remove()
            };
        }

        it('should NOT fire onClick when canSelect was true at attach but false at fire-time', () => {
            const { element, onClick, setCanSelect, cleanup } = setupWithControllableCanSelect(true);

            setCanSelect(false);
            element.dispatchEvent(new MouseEvent('mousedown', { button: 0, bubbles: true }));

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should fire onClick when canSelect was false at attach but true at fire-time', () => {
            const { element, onClick, setCanSelect, cleanup } = setupWithControllableCanSelect(false);

            setCanSelect(true);
            element.dispatchEvent(new MouseEvent('mousedown', { button: 0, bubbles: true }));

            expect(onClick).toHaveBeenCalledTimes(1);

            cleanup();
        });

        it('should NOT fire onDoubleClick when canSelect is false at fire-time', () => {
            const { element, onDoubleClick, setCanSelect, cleanup } = setupWithControllableCanSelect(true);

            setCanSelect(false);
            element.dispatchEvent(new MouseEvent('dblclick', { bubbles: true }));

            expect(onDoubleClick).not.toHaveBeenCalled();

            cleanup();
        });
    });

    describe('drag attachment lifecycle (moveable diff)', () => {
        function makeContext(pg: CanvasProcessGroup, onDragEnd = vi.fn()): ProcessGroupRenderContext {
            const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            container.classList.add('process-group-container');
            document.body.appendChild(container);
            return {
                containerSelection: d3.select(container),
                processGroups: [pg],
                registryClients: [],
                getCanEdit: () => true,
                getCanSelect: () => true,
                getDisabledProcessGroupIds: () => new Set(),
                textEllipsis: { applyEllipsis: vi.fn((sel, text) => sel.text(text)) } as any,
                formatUtils: {} as any,
                nifiCommon: {} as any,
                componentUtils: {
                    bulletins: vi.fn(),
                    activeThreadCount: vi.fn(),
                    comments: vi.fn(),
                    canvasTooltip: vi.fn(),
                    resetCanvasTooltip: vi.fn()
                } as any,
                callbacks: { onDragEnd }
            };
        }

        it('marks writable process groups as moveable on first render and attaches a drag listener', () => {
            const pg = createMockProcessGroup({ entity: { permissions: { canRead: true, canWrite: true } } });
            const ctx = makeContext(pg);

            ProcessGroupRenderer.render(ctx);

            const el = ctx.containerSelection.select<SVGGElement>('g.process-group');
            expect(el.classed('moveable')).toBe(true);
            expect(el.on('mousedown.drag')).toBeTruthy();
        });

        it('does NOT mark process groups as moveable when write permission is missing', () => {
            const pg = createMockProcessGroup({ entity: { permissions: { canRead: true, canWrite: false } } });
            const ctx = makeContext(pg);

            ProcessGroupRenderer.render(ctx);

            const el = ctx.containerSelection.select<SVGGElement>('g.process-group');
            expect(el.classed('moveable')).toBe(false);
            expect(el.on('mousedown.drag')).toBeFalsy();
        });

        it('detaches drag and clears moveable when write permission is revoked between renders', () => {
            const pg = createMockProcessGroup({ entity: { permissions: { canRead: true, canWrite: true } } });
            const ctx = makeContext(pg);

            ProcessGroupRenderer.render(ctx);
            const el = ctx.containerSelection.select<SVGGElement>('g.process-group');
            expect(el.classed('moveable')).toBe(true);
            expect(el.on('mousedown.drag')).toBeTruthy();

            pg.entity.permissions.canWrite = false;
            ProcessGroupRenderer.render(ctx);

            expect(el.classed('moveable')).toBe(false);
            expect(el.on('mousedown.drag')).toBeFalsy();
        });

        it('reattaches drag when write permission is granted between renders', () => {
            const pg = createMockProcessGroup({ entity: { permissions: { canRead: true, canWrite: false } } });
            const ctx = makeContext(pg);

            ProcessGroupRenderer.render(ctx);
            const el = ctx.containerSelection.select<SVGGElement>('g.process-group');
            expect(el.classed('moveable')).toBe(false);

            pg.entity.permissions.canWrite = true;
            ProcessGroupRenderer.render(ctx);

            expect(el.classed('moveable')).toBe(true);
            expect(el.on('mousedown.drag')).toBeTruthy();
        });

        it('does NOT re-attach the drag listener on a no-op steady-state render', () => {
            const pg = createMockProcessGroup({ entity: { permissions: { canRead: true, canWrite: true } } });
            const ctx = makeContext(pg);

            ProcessGroupRenderer.render(ctx);
            const el = ctx.containerSelection.select<SVGGElement>('g.process-group');
            const firstHandler = el.on('mousedown.drag');
            expect(firstHandler).toBeTruthy();

            ProcessGroupRenderer.render(ctx);

            // Same listener instance: the moveable diff suppresses both d3.drag()
            // construction and the .call(drag) re-attach.
            expect(el.on('mousedown.drag')).toBe(firstHandler);
        });
    });

    // The mousedown.selection handler is attached ONCE per element. The
    // `getCanSelect` getter MUST be re-read at fire-time so consumers that
    // toggle selection mode at runtime get correct behavior without
    // re-attachment churn.
    describe('canSelect runtime gating (attach-once getter)', () => {
        it('does NOT fire onClick when canSelect was true at attach but false at fire-time', async () => {
            const onClick = vi.fn();
            const pg = createMockProcessGroup({ entity: { id: 'pg-1' } });
            const options: SetupOptions = { processGroups: [pg], canSelect: true, callbacks: { onClick } };

            const { getProcessGroupElements, cleanup } = await setup(options);

            options.canSelect = false;
            getProcessGroupElements()
                .node()!
                .dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0 }));

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('fires onClick when canSelect was false at attach but true at fire-time', async () => {
            const onClick = vi.fn();
            const pg = createMockProcessGroup({ entity: { id: 'pg-1' } });
            const options: SetupOptions = { processGroups: [pg], canSelect: false, callbacks: { onClick } };

            const { getProcessGroupElements, cleanup } = await setup(options);

            options.canSelect = true;
            getProcessGroupElements()
                .node()!
                .dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0 }));

            expect(onClick).toHaveBeenCalledTimes(1);

            cleanup();
        });
    });

    // The drag filter is attached ONCE per element. The
    // `getDisabledProcessGroupIds` getter MUST be re-read at fire-time so
    // saving-state toggles take effect immediately without forcing a re-render
    // to refresh a captured field.
    describe('drag filter live disabledProcessGroupIds (attach-once getter)', () => {
        it('blocks drag-end when the live disabled set includes the id, even though the filter was attached when the set was empty', async () => {
            const onDragEnd = vi.fn();
            const pg = createMockProcessGroup({ entity: { id: 'pg-1' } });
            const disabledProcessGroupIds = new Set<string>();

            const { getProcessGroupElements, cleanup } = await setup({
                processGroups: [pg],
                disabledProcessGroupIds,
                callbacks: { onDragEnd }
            });

            const el = getProcessGroupElements().node()!;

            el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));
            window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));
            expect(onDragEnd).toHaveBeenCalledTimes(1);

            onDragEnd.mockClear();
            disabledProcessGroupIds.add('pg-1');

            el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));
            window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));
            expect(onDragEnd).not.toHaveBeenCalled();

            cleanup();
        });
    });
});
