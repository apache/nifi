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
import { ProcessorRenderer } from './processor-renderer';
import { ProcessorRenderContext } from '../render-context.types';
import { CanvasProcessor } from '../../canvas.types';
import { ComponentType } from '@nifi/shared';

/**
 * Test setup options for ProcessorRenderer tests
 */
interface SetupOptions {
    processors?: CanvasProcessor[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledProcessorIds?: Set<string>;
    previewExtensions?: any[];
    callbacks?: Partial<ProcessorRenderContext['callbacks']>;
}

/**
 * Creates a mock processor entity for testing
 */
function createMockProcessor(
    overrides: {
        entity?: {
            id?: string;
            position?: { x: number; y: number };
            permissions?: { canRead?: boolean; canWrite?: boolean };
            component?: {
                name?: string;
                type?: string;
                bundle?: { group?: string; artifact?: string; version?: string };
                style?: Record<string, string>;
                config?: { comments?: string };
                validationErrors?: string[];
                restricted?: boolean;
            };
            status?: {
                aggregateSnapshot?: {
                    runStatus?: string;
                    input?: string;
                    read?: string;
                    written?: string;
                    output?: string;
                    tasks?: string;
                    tasksDuration?: string;
                    executionNode?: string;
                };
            };
            bulletins?: any[];
        };
        ui?: {
            dimensions?: { width: number; height: number };
            currentPosition?: { x: number; y: number };
        };
    } = {}
): CanvasProcessor {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    const defaultProcessor: CanvasProcessor = {
        entity: {
            id: entityOverrides.id ?? 'processor-1',
            position: entityOverrides.position ?? { x: 100, y: 200 },
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            },
            component: {
                name: entityOverrides.component?.name ?? 'Test Processor',
                type: entityOverrides.component?.type ?? 'org.apache.nifi.processors.standard.GenerateFlowFile',
                bundle: {
                    group: entityOverrides.component?.bundle?.group ?? 'org.apache.nifi',
                    artifact: entityOverrides.component?.bundle?.artifact ?? 'nifi-standard-nar',
                    version: entityOverrides.component?.bundle?.version ?? '2.0.0'
                },
                style: entityOverrides.component?.style ?? {},
                config: {
                    comments: entityOverrides.component?.config?.comments ?? ''
                },
                validationErrors: entityOverrides.component?.validationErrors ?? [],
                restricted: entityOverrides.component?.restricted ?? false
            },
            status: {
                aggregateSnapshot: {
                    runStatus: entityOverrides.status?.aggregateSnapshot?.runStatus ?? 'Stopped',
                    input: entityOverrides.status?.aggregateSnapshot?.input ?? '0 (0 bytes)',
                    read: entityOverrides.status?.aggregateSnapshot?.read ?? '0 bytes',
                    written: entityOverrides.status?.aggregateSnapshot?.written ?? '0 bytes',
                    output: entityOverrides.status?.aggregateSnapshot?.output ?? '0 (0 bytes)',
                    tasks: entityOverrides.status?.aggregateSnapshot?.tasks ?? '0',
                    tasksDuration: entityOverrides.status?.aggregateSnapshot?.tasksDuration ?? '00:00:00.000',
                    executionNode: entityOverrides.status?.aggregateSnapshot?.executionNode ?? 'ALL'
                }
            },
            bulletins: entityOverrides.bulletins ?? []
        },
        ui: {
            componentType: ComponentType.Processor,
            dimensions: uiOverrides.dimensions ?? { width: 352, height: 128 },
            currentPosition: uiOverrides.currentPosition
        }
    };

    return defaultProcessor;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): ProcessorRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('processor-container');
    document.body.appendChild(container);

    return {
        containerSelection: d3.select(container),
        processors: options.processors || [],
        canSelect: options.canSelect ?? true,
        disabledProcessorIds: options.disabledProcessorIds,
        previewExtensions: options.previewExtensions || [],
        getCanEdit: () => options.canEdit ?? true,
        textEllipsis: {
            applyEllipsis: vi.fn((selection, text, _className) => {
                selection.text(text);
            }),
            determineContrastColor: vi.fn(() => '#ffffff')
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

    // Render processors
    ProcessorRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getProcessorElements: () => context.containerSelection.selectAll<SVGGElement, CanvasProcessor>('g.processor'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.processor-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('ProcessorRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.processor-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new processors', () => {
            it('should create processor group elements for each processor', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const elements = getProcessorElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-processor-1');
                expect(elements.classed('processor')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create border and body rectangles', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('rect.border').empty()).toBe(false);
                expect(processorEl.select('rect.body').empty()).toBe(false);

                cleanup();
            });

            it('should create icon container and icon', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('rect.processor-icon-container').empty()).toBe(false);
                expect(processorEl.select('text.processor-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create restricted indicator elements', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('circle.restricted-background').empty()).toBe(false);
                expect(processorEl.select('text.restricted').empty()).toBe(false);

                cleanup();
            });

            it('should create is-primary indicator elements', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('circle.is-primary-background').empty()).toBe(false);
                expect(processorEl.select('text.is-primary').empty()).toBe(false);

                cleanup();
            });

            it('should create preview badge elements (hidden by default)', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                const previewBadge = processorEl.select('rect.processor-preview-badge');
                const previewText = processorEl.select('text.processor-preview-text');

                expect(previewBadge.empty()).toBe(false);
                expect(previewText.empty()).toBe(false);
                expect(previewBadge.style('visibility')).toBe('hidden');
                expect(previewText.style('visibility')).toBe('hidden');

                cleanup();
            });

            it('should create processor name element', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('text.processor-name').empty()).toBe(false);

                cleanup();
            });

            it('should create multiple processors', async () => {
                const processor1 = createMockProcessor({ entity: { id: 'proc-1' } });
                const processor2 = createMockProcessor({ entity: { id: 'proc-2' } });
                const processor3 = createMockProcessor({ entity: { id: 'proc-3' } });

                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor1, processor2, processor3]
                });

                expect(getProcessorElements().size()).toBe(3);

                cleanup();
            });
        });

        describe('UPDATE phase - positioning and styling', () => {
            it('should position processors using transform', async () => {
                const processor = createMockProcessor({
                    entity: { position: { x: 150, y: 250 } }
                });
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const transform = getProcessorElements().attr('transform');
                expect(transform).toBe('translate(150, 250)');

                cleanup();
            });

            it('should use ui.currentPosition when set (during drag)', async () => {
                const processor = createMockProcessor({
                    entity: { position: { x: 100, y: 100 } },
                    ui: {
                        dimensions: { width: 352, height: 128 },
                        currentPosition: { x: 200, y: 300 }
                    }
                });
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const transform = getProcessorElements().attr('transform');
                expect(transform).toBe('translate(200, 300)');

                cleanup();
            });

            it('should set dimensions on border and body', async () => {
                const processor = createMockProcessor({
                    ui: {
                        dimensions: { width: 400, height: 150 }
                    }
                });
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('rect.border').attr('width')).toBe('400');
                expect(processorEl.select('rect.border').attr('height')).toBe('150');
                expect(processorEl.select('rect.body').attr('width')).toBe('400');
                expect(processorEl.select('rect.body').attr('height')).toBe('150');

                cleanup();
            });

            it('should apply disabled styling when processor is in disabledProcessorIds', async () => {
                const processor = createMockProcessor({ entity: { id: 'proc-disabled' } });
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor],
                    disabledProcessorIds: new Set(['proc-disabled'])
                });

                const processorEl = getProcessorElements();
                expect(processorEl.classed('disabled')).toBe(true);
                expect(processorEl.style('opacity')).toBe('0.6');
                expect(processorEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor],
                    canSelect: true
                });

                expect(getProcessorElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const processor = createMockProcessor();
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor],
                    canSelect: false
                });

                expect(getProcessorElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const processor = createMockProcessor({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                const processorEl = getProcessorElements();
                expect(processorEl.select('rect.border').classed('unauthorized')).toBe(true);
                expect(processorEl.select('rect.body').classed('unauthorized')).toBe(true);
                expect(processorEl.select('rect.processor-icon-container').classed('unauthorized')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - visible processors with details', () => {
            it('should create details group when processor is visible', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                // Mark processor as visible
                getProcessorElements().classed('visible', true);

                // Re-render to trigger details creation
                ProcessorRenderer.render(context);

                expect(getProcessorElements().select('g.processor-canvas-details').empty()).toBe(false);

                cleanup();
            });

            it('should create run status icon in details', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const details = getProcessorElements().select('g.processor-canvas-details');
                expect(details.select('text.run-status-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create processor type and bundle in details', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const details = getProcessorElements().select('g.processor-canvas-details');
                expect(details.select('text.processor-type').empty()).toBe(false);
                expect(details.select('text.processor-bundle').empty()).toBe(false);

                cleanup();
            });

            it('should create statistics rows in details', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const details = getProcessorElements().select('g.processor-canvas-details');
                expect(details.select('rect.processor-stats-in-out').empty()).toBe(false);
                expect(details.select('rect.processor-read-write-stats').empty()).toBe(false);
                expect(details.select('text.processor-in').empty()).toBe(false);
                expect(details.select('text.processor-out').empty()).toBe(false);
                expect(details.select('text.processor-read-write').empty()).toBe(false);
                expect(details.select('text.processor-tasks-time').empty()).toBe(false);

                cleanup();
            });

            it('should create bulletin elements in details', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const details = getProcessorElements().select('g.processor-canvas-details');
                expect(details.select('rect.bulletin-background').empty()).toBe(false);
                expect(details.select('text.bulletin-icon').empty()).toBe(false);

                cleanup();
            });

            it('should create active thread count elements in details', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const details = getProcessorElements().select('g.processor-canvas-details');
                expect(details.select('text.active-thread-count-icon').empty()).toBe(false);
                expect(details.select('text.active-thread-count').empty()).toBe(false);

                cleanup();
            });

            it('should create comment icon in details', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const details = getProcessorElements().select('g.processor-canvas-details');
                expect(details.select('text.component-comments').empty()).toBe(false);

                cleanup();
            });

            it('should remove details when processor becomes not visible', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                // First make visible and render details
                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);
                expect(getProcessorElements().select('g.processor-canvas-details').empty()).toBe(false);

                // Now make not visible and re-render
                getProcessorElements().classed('visible', false);
                ProcessorRenderer.render(context);
                expect(getProcessorElements().select('g.processor-canvas-details').empty()).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - run status styling', () => {
            it('should show running status with success color', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Running' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const runStatusIcon = getProcessorElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('running')).toBe(true);
                expect(runStatusIcon.classed('success-color-default')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf04b'); // fa-play

                cleanup();
            });

            it('should show stopped status with error color', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Stopped' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const runStatusIcon = getProcessorElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('stopped')).toBe(true);
                expect(runStatusIcon.classed('error-color-variant')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf04d'); // fa-stop

                cleanup();
            });

            it('should show invalid status with caution color', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Invalid' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const runStatusIcon = getProcessorElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('invalid')).toBe(true);
                expect(runStatusIcon.classed('caution-color')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf071'); // fa-warning

                cleanup();
            });

            it('should show disabled status with flowfont', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Disabled' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const runStatusIcon = getProcessorElements().select('text.run-status-icon');
                expect(runStatusIcon.empty()).toBe(false);
                expect(runStatusIcon.text()).toBe('\ue802');

                cleanup();
            });

            it('should show validating status with spinner', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { runStatus: 'Validating' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const runStatusIcon = getProcessorElements().select('text.run-status-icon');
                expect(runStatusIcon.classed('validating')).toBe(true);
                expect(runStatusIcon.classed('fa-spin')).toBe(true);
                expect(runStatusIcon.text()).toBe('\uf1ce'); // fa-spinner

                cleanup();
            });
        });

        describe('UPDATE phase - restricted and primary indicators', () => {
            it('should show restricted indicator when processor is restricted', async () => {
                const processor = createMockProcessor({
                    entity: {
                        component: { restricted: true }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                expect(getProcessorElements().select('circle.restricted-background').style('visibility')).toBe(
                    'visible'
                );
                expect(getProcessorElements().select('text.restricted').style('visibility')).toBe('visible');

                cleanup();
            });

            it('should hide restricted indicator when processor is not restricted', async () => {
                const processor = createMockProcessor({
                    entity: {
                        component: { restricted: false }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                expect(getProcessorElements().select('circle.restricted-background').style('visibility')).toBe(
                    'hidden'
                );
                expect(getProcessorElements().select('text.restricted').style('visibility')).toBe('hidden');

                cleanup();
            });

            it('should show is-primary indicator when executionNode is PRIMARY', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { executionNode: 'PRIMARY' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                expect(getProcessorElements().select('circle.is-primary-background').style('visibility')).toBe(
                    'visible'
                );
                expect(getProcessorElements().select('text.is-primary').style('visibility')).toBe('visible');

                cleanup();
            });

            it('should hide is-primary indicator when executionNode is ALL', async () => {
                const processor = createMockProcessor({
                    entity: {
                        status: {
                            aggregateSnapshot: { executionNode: 'ALL' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                expect(getProcessorElements().select('circle.is-primary-background').style('visibility')).toBe(
                    'hidden'
                );
                expect(getProcessorElements().select('text.is-primary').style('visibility')).toBe('hidden');

                cleanup();
            });
        });

        describe('UPDATE phase - preview badge', () => {
            it('should show preview badge when processor is a preview extension', async () => {
                const processor = createMockProcessor({
                    entity: {
                        component: {
                            type: 'org.apache.nifi.processors.PreviewProcessor',
                            bundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-preview-nar',
                                version: '2.0.0'
                            }
                        }
                    }
                });
                const previewExtensions = [
                    {
                        type: 'org.apache.nifi.processors.PreviewProcessor',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-preview-nar',
                            version: '2.0.0'
                        }
                    }
                ];
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor],
                    previewExtensions
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                expect(getProcessorElements().select('rect.processor-preview-badge').style('visibility')).toBe(
                    'visible'
                );
                expect(getProcessorElements().select('text.processor-preview-text').style('visibility')).toBe(
                    'visible'
                );

                cleanup();
            });

            it('should adjust processor name position when preview badge is shown', async () => {
                const processor = createMockProcessor({
                    entity: {
                        component: {
                            type: 'org.apache.nifi.processors.PreviewProcessor',
                            bundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-preview-nar',
                                version: '2.0.0'
                            }
                        }
                    }
                });
                const previewExtensions = [
                    {
                        type: 'org.apache.nifi.processors.PreviewProcessor',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-preview-nar',
                            version: '2.0.0'
                        }
                    }
                ];
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor],
                    previewExtensions
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                // When preview badge is shown, the preview badge should be visible
                const previewBadge = getProcessorElements().select('rect.processor-preview-badge');
                expect(previewBadge.style('visibility')).toBe('visible');

                cleanup();
            });
        });

        describe('UPDATE phase - custom styling', () => {
            it('should apply custom background color to icon container', async () => {
                const processor = createMockProcessor({
                    entity: {
                        component: {
                            style: { 'background-color': '#ff0000' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const iconContainer = getProcessorElements().select('rect.processor-icon-container');
                // D3 returns the color in hex format when set via style()
                expect(iconContainer.style('fill')).toBe('#ff0000');

                cleanup();
            });

            it('should apply custom stroke color to border', async () => {
                const processor = createMockProcessor({
                    entity: {
                        component: {
                            style: { 'background-color': '#00ff00' }
                        }
                    }
                });
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                getProcessorElements().classed('visible', true);
                ProcessorRenderer.render(context);

                const border = getProcessorElements().select('rect.border');
                // D3 returns the color in hex format when set via style()
                expect(border.style('stroke')).toBe('#00ff00');

                cleanup();
            });
        });

        describe('EXIT phase - removing processors', () => {
            it('should remove processors that are no longer in data', async () => {
                const processor1 = createMockProcessor({ entity: { id: 'proc-1' } });
                const processor2 = createMockProcessor({ entity: { id: 'proc-2' } });

                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor1, processor2]
                });

                expect(getProcessorElements().size()).toBe(2);

                // Re-render with only processor1
                context.processors = [processor1];
                ProcessorRenderer.render(context);

                expect(getProcessorElements().size()).toBe(1);
                expect(getProcessorElements().attr('id')).toBe('id-proc-1');

                cleanup();
            });

            it('should remove all processors when data is empty', async () => {
                const processor = createMockProcessor();
                const { context, getProcessorElements, cleanup } = await setup({
                    processors: [processor]
                });

                expect(getProcessorElements().size()).toBe(1);

                // Re-render with empty array
                context.processors = [];
                ProcessorRenderer.render(context);

                expect(getProcessorElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const processor = createMockProcessor();
            const { getProcessorElements, cleanup } = await setup({
                processors: [processor],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const processorEl = getProcessorElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            processorEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(processor, expect.any(MouseEvent));

            cleanup();
        });

        it('should not attach click handler when canSelect is false', async () => {
            const onClick = vi.fn();
            const processor = createMockProcessor();
            const { getProcessorElements, cleanup } = await setup({
                processors: [processor],
                canSelect: false,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const processorEl = getProcessorElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            processorEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should ignore right-click events', async () => {
            const onClick = vi.fn();
            const processor = createMockProcessor();
            const { getProcessorElements, cleanup } = await setup({
                processors: [processor],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate right-click (button: 2)
            const processorEl = getProcessorElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            processorEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should attach double-click handler when onDoubleClick callback provided', async () => {
            const onDoubleClick = vi.fn();
            const processor = createMockProcessor();
            const { getProcessorElements, cleanup } = await setup({
                processors: [processor],
                canSelect: true,
                callbacks: { onDoubleClick }
            });

            // Simulate dblclick event
            const processorEl = getProcessorElements().node()!;
            const event = new MouseEvent('dblclick', { bubbles: true });
            processorEl.dispatchEvent(event);

            expect(onDoubleClick).toHaveBeenCalledTimes(1);
            expect(onDoubleClick).toHaveBeenCalledWith(processor, expect.any(MouseEvent));

            cleanup();
        });
    });

    describe('pan', () => {
        it('should update processor elements for entering/leaving processors', async () => {
            const processor = createMockProcessor({
                entity: { position: { x: 500, y: 600 } }
            });
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            // Mark as entering (becoming visible)
            getProcessorElements().classed('entering', true).classed('visible', true);

            // Call pan to update
            ProcessorRenderer.pan(getProcessorElements(), context);

            // Should have created details since it's now visible
            expect(getProcessorElements().select('g.processor-canvas-details').empty()).toBe(false);

            cleanup();
        });

        it('should remove details for leaving processors', async () => {
            const processor = createMockProcessor();
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            // First make visible
            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);
            expect(getProcessorElements().select('g.processor-canvas-details').empty()).toBe(false);

            // Now mark as leaving (becoming not visible)
            getProcessorElements().classed('visible', false).classed('leaving', true);

            // Call pan to update
            ProcessorRenderer.pan(getProcessorElements(), context);

            // Details should be removed
            expect(getProcessorElements().select('g.processor-canvas-details').empty()).toBe(true);

            cleanup();
        });
    });

    describe('statistics rendering', () => {
        it('should format and display input statistics', async () => {
            const processor = createMockProcessor({
                entity: {
                    status: {
                        aggregateSnapshot: {
                            runStatus: 'Running',
                            input: '100 (1.5 MB)'
                        }
                    }
                }
            });
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);

            expect(context.formatUtils.formatQueuedStats).toHaveBeenCalledWith('100 (1.5 MB)');

            cleanup();
        });

        it('should display read/write statistics', async () => {
            const processor = createMockProcessor({
                entity: {
                    status: {
                        aggregateSnapshot: {
                            runStatus: 'Running',
                            read: '500 KB',
                            written: '1.2 MB'
                        }
                    }
                }
            });
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);

            const readWriteText = getProcessorElements().select('text.processor-read-write').text();
            expect(readWriteText).toBe('500 KB / 1.2 MB');

            cleanup();
        });

        it('should display tasks/time statistics', async () => {
            const processor = createMockProcessor({
                entity: {
                    status: {
                        aggregateSnapshot: {
                            runStatus: 'Running',
                            tasks: '42',
                            tasksDuration: '00:05:30.123'
                        }
                    }
                }
            });
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);

            const tasksTimeText = getProcessorElements().select('text.processor-tasks-time').text();
            expect(tasksTimeText).toBe('42 / 00:05:30.123');

            cleanup();
        });
    });

    describe('componentUtils integration', () => {
        it('should call bulletins utility', async () => {
            const processor = createMockProcessor({
                entity: {
                    bulletins: [{ id: 1, message: 'Test bulletin' }]
                }
            });
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);

            expect(context.componentUtils.bulletins).toHaveBeenCalled();

            cleanup();
        });

        it('should call activeThreadCount utility', async () => {
            const processor = createMockProcessor();
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);

            expect(context.componentUtils.activeThreadCount).toHaveBeenCalled();

            cleanup();
        });

        it('should call comments utility when processor has read permission', async () => {
            const processor = createMockProcessor({
                entity: {
                    component: {
                        config: { comments: 'Test comment' }
                    }
                }
            });
            const { context, getProcessorElements, cleanup } = await setup({
                processors: [processor]
            });

            getProcessorElements().classed('visible', true);
            ProcessorRenderer.render(context);

            expect(context.componentUtils.comments).toHaveBeenCalledWith(expect.anything(), 'Test comment');

            cleanup();
        });
    });
});
