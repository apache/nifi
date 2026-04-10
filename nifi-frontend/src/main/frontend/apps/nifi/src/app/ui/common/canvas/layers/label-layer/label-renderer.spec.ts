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
import { LabelRenderer } from './label-renderer';
import { LabelRenderContext } from '../render-context.types';
import { CanvasLabel } from '../../canvas.types';
import { ComponentType } from '@nifi/shared';

/**
 * Test setup options for LabelRenderer tests
 */
interface SetupOptions {
    labels?: CanvasLabel[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledLabelIds?: Set<string>;
    scale?: number;
    callbacks?: Partial<LabelRenderContext['callbacks']>;
}

/**
 * Creates a mock label entity for testing
 */
function createMockLabel(
    overrides: {
        entity?: {
            id?: string;
            position?: { x: number; y: number };
            permissions?: { canRead?: boolean; canWrite?: boolean };
            zIndex?: number;
            component?: {
                label?: string;
                style?: {
                    'background-color'?: string;
                    'font-size'?: string;
                };
            };
        };
        ui?: {
            dimensions?: { width: number; height: number };
            currentPosition?: { x: number; y: number };
        };
    } = {}
): CanvasLabel {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    const defaultLabel: CanvasLabel = {
        entity: {
            id: entityOverrides.id ?? 'label-1',
            position: entityOverrides.position ?? { x: 100, y: 200 },
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            },
            zIndex: entityOverrides.zIndex ?? 0,
            component: {
                label: entityOverrides.component?.label ?? 'Test Label',
                style: entityOverrides.component?.style ?? {}
            }
        },
        ui: {
            componentType: ComponentType.Label,
            dimensions: uiOverrides.dimensions ?? { width: 150, height: 150 },
            currentPosition: uiOverrides.currentPosition
        }
    };

    return defaultLabel;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): LabelRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('label-container');
    document.body.appendChild(container);

    return {
        containerSelection: d3.select(container),
        labels: options.labels || [],
        canSelect: options.canSelect ?? true,
        disabledLabelIds: options.disabledLabelIds,
        scale: options.scale ?? 1,
        getCanEdit: () => options.canEdit ?? true,
        textEllipsis: {
            applyEllipsis: vi.fn((selection, text, _className) => {
                selection.text(text);
            }),
            determineContrastColor: vi.fn((bgColor) => {
                // Simple mock: return black for light colors, white for dark
                return bgColor === '#ffffff' || bgColor === '#ffffcc' ? '#000000' : '#ffffff';
            })
        } as any,
        formatUtils: {} as any,
        nifiCommon: {
            compareNumber: vi.fn((a, b) => (a ?? 0) - (b ?? 0))
        } as any,
        callbacks: {
            onClick: options.callbacks?.onClick,
            onDoubleClick: options.callbacks?.onDoubleClick,
            onResizeEnd: options.callbacks?.onResizeEnd,
            onDragEnd: options.callbacks?.onDragEnd
        }
    };
}

/**
 * SIFERS-style async setup function
 */
async function setup(options: SetupOptions = {}) {
    const context = createMockContext(options);

    // Render labels
    LabelRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getLabelElements: () => context.containerSelection.selectAll<SVGGElement, CanvasLabel>('g.label'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.label-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('LabelRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.label-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new labels', () => {
            it('should create label elements', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const elements = getLabelElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-label-1');
                expect(elements.classed('label')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create border and body rectangles', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const labelEl = getLabelElements();
                expect(labelEl.select('rect.border').empty()).toBe(false);
                expect(labelEl.select('rect.body').empty()).toBe(false);

                cleanup();
            });

            it('should create label text element', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const labelText = getLabelElements().select('text.label-value');
                expect(labelText.empty()).toBe(false);

                cleanup();
            });

            it('should create resize handle', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const resizeHandle = getLabelElements().select('path.resizable-triangle');
                expect(resizeHandle.empty()).toBe(false);
                expect(resizeHandle.style('cursor')).toBe('nwse-resize');

                cleanup();
            });

            it('should create multiple labels', async () => {
                const label1 = createMockLabel({ entity: { id: 'label-1' } });
                const label2 = createMockLabel({ entity: { id: 'label-2' } });
                const label3 = createMockLabel({ entity: { id: 'label-3' } });

                const { getLabelElements, cleanup } = await setup({
                    labels: [label1, label2, label3]
                });

                expect(getLabelElements().size()).toBe(3);

                cleanup();
            });
        });

        describe('UPDATE phase - positioning and styling', () => {
            it('should position labels using transform', async () => {
                const label = createMockLabel({
                    entity: { position: { x: 150, y: 250 } }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const transform = getLabelElements().attr('transform');
                expect(transform).toBe('translate(150, 250)');

                cleanup();
            });

            it('should use ui.currentPosition when set (during drag)', async () => {
                const label = createMockLabel({
                    entity: { position: { x: 100, y: 100 } },
                    ui: {
                        dimensions: { width: 150, height: 150 },
                        currentPosition: { x: 200, y: 300 }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const transform = getLabelElements().attr('transform');
                expect(transform).toBe('translate(200, 300)');

                cleanup();
            });

            it('should apply disabled styling when label is in disabledLabelIds', async () => {
                const label = createMockLabel({ entity: { id: 'label-disabled' } });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label],
                    disabledLabelIds: new Set(['label-disabled'])
                });

                const labelEl = getLabelElements();
                expect(labelEl.classed('disabled')).toBe(true);
                expect(labelEl.style('opacity')).toBe('0.6');
                expect(labelEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label],
                    canSelect: true
                });

                expect(getLabelElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label],
                    canSelect: false
                });

                expect(getLabelElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const label = createMockLabel({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const labelEl = getLabelElements();
                expect(labelEl.select('rect.border').classed('unauthorized')).toBe(true);
                expect(labelEl.select('rect.body').classed('unauthorized')).toBe(true);

                cleanup();
            });

            it('should set border and body dimensions correctly', async () => {
                const label = createMockLabel({
                    ui: { dimensions: { width: 200, height: 100 } }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const border = getLabelElements().select('rect.border');
                const body = getLabelElements().select('rect.body');

                expect(border.attr('width')).toBe('200');
                expect(border.attr('height')).toBe('100');
                expect(body.attr('width')).toBe('200');
                expect(body.attr('height')).toBe('100');

                cleanup();
            });
        });

        describe('UPDATE phase - label text and styling', () => {
            it('should apply custom background color', async () => {
                const label = createMockLabel({
                    entity: {
                        component: {
                            label: 'Test',
                            style: { 'background-color': '#ff0000' }
                        }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const body = getLabelElements().select('rect.body');
                expect(body.style('fill')).toBe('#ff0000');

                cleanup();
            });

            it('should apply default background color when no custom color specified', async () => {
                const label = createMockLabel({
                    entity: {
                        component: {
                            label: 'Test',
                            style: {}
                        }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const body = getLabelElements().select('rect.body');
                // Default color is #fff7d7 (LABEL_DEFAULT_COLOR)
                expect(body.style('fill')).toBe('#fff7d7');

                cleanup();
            });

            it('should apply custom font size', async () => {
                const label = createMockLabel({
                    entity: {
                        component: {
                            label: 'Test',
                            style: { 'font-size': '24px' }
                        }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const labelText = getLabelElements().select('text.label-value');
                expect(labelText.attr('font-size')).toBe('24px');

                cleanup();
            });

            it('should apply default font size when no custom size specified', async () => {
                const label = createMockLabel({
                    entity: {
                        component: {
                            label: 'Test',
                            style: {}
                        }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const labelText = getLabelElements().select('text.label-value');
                expect(labelText.attr('font-size')).toBe('12px');

                cleanup();
            });

            it('should call determineContrastColor for text color', async () => {
                const label = createMockLabel({
                    entity: {
                        component: {
                            label: 'Test',
                            style: { 'background-color': '#000000' }
                        }
                    }
                });
                const { context, cleanup } = await setup({
                    labels: [label]
                });

                expect(context.textEllipsis.determineContrastColor).toHaveBeenCalledWith('#000000');

                cleanup();
            });

            it('should clear text for unauthorized labels', async () => {
                const label = createMockLabel({
                    entity: {
                        permissions: { canRead: false, canWrite: false },
                        component: { label: 'Secret Label' }
                    }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const labelText = getLabelElements().select('text.label-value');
                expect(labelText.selectAll('tspan').size()).toBe(0);

                cleanup();
            });
        });

        describe('UPDATE phase - resize handle', () => {
            it('should position resize handle in bottom-right corner', async () => {
                const label = createMockLabel({
                    ui: { dimensions: { width: 200, height: 100 } }
                });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                const resizeHandle = getLabelElements().select('path.resizable-triangle');
                expect(resizeHandle.attr('transform')).toBe('translate(198, 90)');

                cleanup();
            });

            it('should hide resize handle when canSelect is false', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label],
                    canSelect: false
                });

                const resizeHandle = getLabelElements().select('path.resizable-triangle');
                expect(resizeHandle.style('display')).toBe('none');

                cleanup();
            });

            it('should hide resize handle when canEdit is false', async () => {
                const label = createMockLabel();
                const { getLabelElements, cleanup } = await setup({
                    labels: [label],
                    canSelect: true,
                    canEdit: false
                });

                const resizeHandle = getLabelElements().select('path.resizable-triangle');
                expect(resizeHandle.style('display')).toBe('none');

                cleanup();
            });

            it('should hide resize handle when label is disabled', async () => {
                const label = createMockLabel({ entity: { id: 'label-1' } });
                const { getLabelElements, cleanup } = await setup({
                    labels: [label],
                    disabledLabelIds: new Set(['label-1'])
                });

                const resizeHandle = getLabelElements().select('path.resizable-triangle');
                expect(resizeHandle.style('display')).toBe('none');

                cleanup();
            });
        });

        describe('UPDATE phase - z-index sorting', () => {
            it('should sort labels by zIndex', async () => {
                const label1 = createMockLabel({ entity: { id: 'label-1', zIndex: 2 } });
                const label2 = createMockLabel({ entity: { id: 'label-2', zIndex: 0 } });
                const label3 = createMockLabel({ entity: { id: 'label-3', zIndex: 1 } });

                const { context, cleanup } = await setup({
                    labels: [label1, label2, label3]
                });

                expect(context.nifiCommon.compareNumber).toHaveBeenCalled();

                cleanup();
            });
        });

        describe('EXIT phase - removing labels', () => {
            it('should remove labels that are no longer in data', async () => {
                const label1 = createMockLabel({ entity: { id: 'label-1' } });
                const label2 = createMockLabel({ entity: { id: 'label-2' } });

                const { context, getLabelElements, cleanup } = await setup({
                    labels: [label1, label2]
                });

                expect(getLabelElements().size()).toBe(2);

                // Re-render with only label1
                context.labels = [label1];
                LabelRenderer.render(context);

                expect(getLabelElements().size()).toBe(1);
                expect(getLabelElements().attr('id')).toBe('id-label-1');

                cleanup();
            });

            it('should remove all labels when data is empty', async () => {
                const label = createMockLabel();
                const { context, getLabelElements, cleanup } = await setup({
                    labels: [label]
                });

                expect(getLabelElements().size()).toBe(1);

                // Re-render with empty array
                context.labels = [];
                LabelRenderer.render(context);

                expect(getLabelElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const label = createMockLabel();
            const { getLabelElements, cleanup } = await setup({
                labels: [label],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const labelEl = getLabelElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            labelEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(label, expect.any(MouseEvent));

            cleanup();
        });

        it('should not attach click handler when canSelect is false', async () => {
            const onClick = vi.fn();
            const label = createMockLabel();
            const { getLabelElements, cleanup } = await setup({
                labels: [label],
                canSelect: false,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const labelEl = getLabelElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            labelEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should ignore right-click events', async () => {
            const onClick = vi.fn();
            const label = createMockLabel();
            const { getLabelElements, cleanup } = await setup({
                labels: [label],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate right-click (button: 2)
            const labelEl = getLabelElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            labelEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should attach double-click handler when onDoubleClick callback provided', async () => {
            const onDoubleClick = vi.fn();
            const label = createMockLabel();
            const { getLabelElements, cleanup } = await setup({
                labels: [label],
                canSelect: true,
                callbacks: { onDoubleClick }
            });

            // Simulate dblclick event
            const labelEl = getLabelElements().node()!;
            const event = new MouseEvent('dblclick', { bubbles: true });
            labelEl.dispatchEvent(event);

            expect(onDoubleClick).toHaveBeenCalledTimes(1);
            expect(onDoubleClick).toHaveBeenCalledWith(label, expect.any(MouseEvent));

            cleanup();
        });
    });

    describe('pan', () => {
        it('should update label elements during pan', async () => {
            const label = createMockLabel({
                entity: { position: { x: 500, y: 600 } }
            });
            const { context, getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            // Mark as entering (becoming visible)
            getLabelElements().classed('entering', true).classed('visible', true);

            // Call pan to update
            LabelRenderer.pan(getLabelElements(), context);

            // Label should still have correct transform
            expect(getLabelElements().attr('transform')).toBe('translate(500, 600)');

            cleanup();
        });

        it('should update cursor during pan based on canSelect', async () => {
            const label = createMockLabel();
            const { context, getLabelElements, cleanup } = await setup({
                labels: [label],
                canSelect: true
            });

            // Call pan
            LabelRenderer.pan(getLabelElements(), context);

            expect(getLabelElements().style('cursor')).toBe('pointer');

            cleanup();
        });

        it('should handle empty selection gracefully', async () => {
            const { context, cleanup } = await setup({
                labels: []
            });

            // Call pan with empty selection - should not throw
            const emptySelection = context.containerSelection.selectAll<SVGGElement, CanvasLabel>('g.label');
            expect(() => LabelRenderer.pan(emptySelection, context)).not.toThrow();

            cleanup();
        });
    });

    describe('data join behavior', () => {
        it('should update existing labels without recreating them', async () => {
            const label = createMockLabel({
                entity: { id: 'label-1', position: { x: 100, y: 100 } }
            });
            const { context, getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            // Get reference to the DOM element
            const originalElement = getLabelElements().node();

            // Update position and re-render
            label.entity.position = { x: 200, y: 200 };
            context.labels = [label];
            LabelRenderer.render(context);

            // Should be the same DOM element
            expect(getLabelElements().node()).toBe(originalElement);
            // But with updated transform
            expect(getLabelElements().attr('transform')).toBe('translate(200, 200)');

            cleanup();
        });

        it('should add new labels while keeping existing ones', async () => {
            const label1 = createMockLabel({ entity: { id: 'label-1' } });
            const { context, getLabelElements, cleanup } = await setup({
                labels: [label1]
            });

            expect(getLabelElements().size()).toBe(1);

            // Add a second label
            const label2 = createMockLabel({ entity: { id: 'label-2' } });
            context.labels = [label1, label2];
            LabelRenderer.render(context);

            expect(getLabelElements().size()).toBe(2);

            cleanup();
        });
    });

    describe('disabled state', () => {
        it('should not be disabled by default', async () => {
            const label = createMockLabel();
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            const labelEl = getLabelElements();
            expect(labelEl.classed('disabled')).toBe(false);
            expect(labelEl.style('opacity')).toBe('');
            expect(labelEl.style('cursor')).toBe('pointer');

            cleanup();
        });

        it('should apply disabled state when label ID is in disabledLabelIds', async () => {
            const label = createMockLabel({ entity: { id: 'label-1' } });
            const { getLabelElements, cleanup } = await setup({
                labels: [label],
                disabledLabelIds: new Set(['label-1'])
            });

            const labelEl = getLabelElements();
            expect(labelEl.classed('disabled')).toBe(true);
            expect(labelEl.style('opacity')).toBe('0.6');
            expect(labelEl.style('cursor')).toBe('not-allowed');

            cleanup();
        });

        it('should not apply disabled state when label ID is not in disabledLabelIds', async () => {
            const label = createMockLabel({ entity: { id: 'label-1' } });
            const { getLabelElements, cleanup } = await setup({
                labels: [label],
                disabledLabelIds: new Set(['label-2', 'label-3']) // Different IDs
            });

            const labelEl = getLabelElements();
            expect(labelEl.classed('disabled')).toBe(false);

            cleanup();
        });

        it('should update disabled state on re-render', async () => {
            const label = createMockLabel({ entity: { id: 'label-1' } });
            const { context, getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            // Initially not disabled
            expect(getLabelElements().classed('disabled')).toBe(false);

            // Add to disabled set and re-render
            context.disabledLabelIds = new Set(['label-1']);
            LabelRenderer.render(context);

            expect(getLabelElements().classed('disabled')).toBe(true);

            // Remove from disabled set and re-render
            context.disabledLabelIds = new Set();
            LabelRenderer.render(context);

            expect(getLabelElements().classed('disabled')).toBe(false);

            cleanup();
        });
    });

    describe('authorization styling', () => {
        it('should apply unauthorized class to border and body when canRead is false', async () => {
            const label = createMockLabel({
                entity: {
                    permissions: { canRead: false, canWrite: false }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            expect(getLabelElements().select('rect.border').classed('unauthorized')).toBe(true);
            expect(getLabelElements().select('rect.body').classed('unauthorized')).toBe(true);

            cleanup();
        });

        it('should not apply unauthorized class when canRead is true', async () => {
            const label = createMockLabel({
                entity: {
                    permissions: { canRead: true, canWrite: false }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            expect(getLabelElements().select('rect.border').classed('unauthorized')).toBe(false);
            expect(getLabelElements().select('rect.body').classed('unauthorized')).toBe(false);

            cleanup();
        });

        it('should not apply fill color when canRead is false', async () => {
            const label = createMockLabel({
                entity: {
                    permissions: { canRead: false, canWrite: false },
                    component: {
                        style: { 'background-color': '#ff0000' }
                    }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            const body = getLabelElements().select('rect.body');
            // When canRead is false, fill should be null (CSS will handle it)
            expect(body.style('fill')).toBe('');

            cleanup();
        });
    });

    describe('multi-line text handling', () => {
        it('should create tspan elements for single line text', async () => {
            const label = createMockLabel({
                entity: {
                    component: { label: 'Single line' }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            const labelText = getLabelElements().select('text.label-value');
            const tspans = labelText.selectAll('tspan');

            // Should have at least one tspan for the text
            expect(tspans.size()).toBeGreaterThan(0);

            cleanup();
        });

        it('should create separate tspans for each line in multi-line text', async () => {
            const label = createMockLabel({
                entity: {
                    component: { label: 'Line 1\nLine 2\nLine 3' }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            const labelText = getLabelElements().select('text.label-value');
            const tspans = labelText.selectAll('tspan');

            // Should have at least 3 tspans for 3 lines
            // Note: In JSDOM, text wrapping calculations may not work correctly,
            // so we verify the structure rather than exact count
            expect(tspans.size()).toBeGreaterThanOrEqual(3);

            cleanup();
        });

        it('should handle empty label text gracefully', async () => {
            const label = createMockLabel({
                entity: {
                    component: { label: '' }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            const labelText = getLabelElements().select('text.label-value');
            // Empty text should still create at least one tspan (for the empty line)
            expect(labelText.selectAll('tspan').size()).toBeGreaterThanOrEqual(1);

            cleanup();
        });

        it('should handle text with only whitespace', async () => {
            const label = createMockLabel({
                entity: {
                    component: { label: '   ' }
                }
            });
            const { getLabelElements, cleanup } = await setup({
                labels: [label]
            });

            const labelText = getLabelElements().select('text.label-value');
            // Whitespace-only text should still render
            expect(labelText.selectAll('tspan').size()).toBeGreaterThanOrEqual(1);

            cleanup();
        });
    });
});
