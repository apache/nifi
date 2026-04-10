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
import { FunnelRenderer } from './funnel-renderer';
import { FunnelRenderContext } from '../render-context.types';
import { CanvasFunnel } from '../../canvas.types';
import { ComponentType } from '@nifi/shared';

/**
 * Test setup options for FunnelRenderer tests
 */
interface SetupOptions {
    funnels?: CanvasFunnel[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledFunnelIds?: Set<string>;
    callbacks?: Partial<FunnelRenderContext['callbacks']>;
}

/**
 * Creates a mock funnel entity for testing
 */
function createMockFunnel(
    overrides: {
        entity?: {
            id?: string;
            position?: { x: number; y: number };
            permissions?: { canRead?: boolean; canWrite?: boolean };
        };
        ui?: {
            dimensions?: { width: number; height: number };
            currentPosition?: { x: number; y: number };
        };
    } = {}
): CanvasFunnel {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    const defaultFunnel: CanvasFunnel = {
        entity: {
            id: entityOverrides.id ?? 'funnel-1',
            position: entityOverrides.position ?? { x: 100, y: 200 },
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            }
        },
        ui: {
            componentType: ComponentType.Funnel,
            dimensions: uiOverrides.dimensions ?? { width: 48, height: 48 },
            currentPosition: uiOverrides.currentPosition
        }
    };

    return defaultFunnel;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): FunnelRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('funnel-container');
    document.body.appendChild(container);

    return {
        containerSelection: d3.select(container),
        funnels: options.funnels || [],
        canSelect: options.canSelect ?? true,
        disabledFunnelIds: options.disabledFunnelIds,
        getCanEdit: () => options.canEdit ?? true,
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

    // Render funnels
    FunnelRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getFunnelElements: () => context.containerSelection.selectAll<SVGGElement, CanvasFunnel>('g.funnel'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.funnel-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('FunnelRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.funnel-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new funnels', () => {
            it('should create funnel elements', async () => {
                const funnel = createMockFunnel();
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const elements = getFunnelElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-funnel-1');
                expect(elements.classed('funnel')).toBe(true);
                expect(elements.classed('component')).toBe(true);

                cleanup();
            });

            it('should create border and body rectangles', async () => {
                const funnel = createMockFunnel();
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const funnelEl = getFunnelElements();
                expect(funnelEl.select('rect.border').empty()).toBe(false);
                expect(funnelEl.select('rect.body').empty()).toBe(false);

                cleanup();
            });

            it('should create funnel icon', async () => {
                const funnel = createMockFunnel();
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const funnelIcon = getFunnelElements().select('text.funnel-icon');
                expect(funnelIcon.empty()).toBe(false);
                expect(funnelIcon.text()).toBe('\ue803'); // Funnel icon from flowfont

                cleanup();
            });

            it('should create funnel icon', async () => {
                const funnel = createMockFunnel();
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const funnelIcon = getFunnelElements().select('text.funnel-icon');
                expect(funnelIcon.empty()).toBe(false);

                cleanup();
            });

            it('should set border and body dimensions correctly', async () => {
                const funnel = createMockFunnel({
                    ui: { dimensions: { width: 48, height: 48 } }
                });
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const border = getFunnelElements().select('rect.border');
                const body = getFunnelElements().select('rect.body');

                expect(border.attr('width')).toBe('48');
                expect(border.attr('height')).toBe('48');
                expect(body.attr('width')).toBe('48');
                expect(body.attr('height')).toBe('48');

                cleanup();
            });

            it('should create multiple funnels', async () => {
                const funnel1 = createMockFunnel({ entity: { id: 'funnel-1' } });
                const funnel2 = createMockFunnel({ entity: { id: 'funnel-2' } });
                const funnel3 = createMockFunnel({ entity: { id: 'funnel-3' } });

                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel1, funnel2, funnel3]
                });

                expect(getFunnelElements().size()).toBe(3);

                cleanup();
            });
        });

        describe('UPDATE phase - positioning and styling', () => {
            it('should position funnels using transform', async () => {
                const funnel = createMockFunnel({
                    entity: { position: { x: 150, y: 250 } }
                });
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const transform = getFunnelElements().attr('transform');
                expect(transform).toBe('translate(150, 250)');

                cleanup();
            });

            it('should use ui.currentPosition when set (during drag)', async () => {
                const funnel = createMockFunnel({
                    entity: { position: { x: 100, y: 100 } },
                    ui: {
                        dimensions: { width: 48, height: 48 },
                        currentPosition: { x: 200, y: 300 }
                    }
                });
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const transform = getFunnelElements().attr('transform');
                expect(transform).toBe('translate(200, 300)');

                cleanup();
            });

            it('should apply disabled styling when funnel is in disabledFunnelIds', async () => {
                const funnel = createMockFunnel({ entity: { id: 'funnel-disabled' } });
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel],
                    disabledFunnelIds: new Set(['funnel-disabled'])
                });

                const funnelEl = getFunnelElements();
                expect(funnelEl.classed('disabled')).toBe(true);
                expect(funnelEl.style('opacity')).toBe('0.6');
                expect(funnelEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const funnel = createMockFunnel();
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel],
                    canSelect: true
                });

                expect(getFunnelElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const funnel = createMockFunnel();
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel],
                    canSelect: false
                });

                expect(getFunnelElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const funnel = createMockFunnel({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const funnelEl = getFunnelElements();
                expect(funnelEl.select('rect.border').classed('unauthorized')).toBe(true);
                expect(funnelEl.select('rect.body').classed('unauthorized')).toBe(true);

                cleanup();
            });

            it('should not apply unauthorized class when canRead is true', async () => {
                const funnel = createMockFunnel({
                    entity: {
                        permissions: { canRead: true, canWrite: true }
                    }
                });
                const { getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                const funnelEl = getFunnelElements();
                expect(funnelEl.select('rect.border').classed('unauthorized')).toBe(false);
                expect(funnelEl.select('rect.body').classed('unauthorized')).toBe(false);

                cleanup();
            });
        });

        describe('EXIT phase - removing funnels', () => {
            it('should remove funnels that are no longer in data', async () => {
                const funnel1 = createMockFunnel({ entity: { id: 'funnel-1' } });
                const funnel2 = createMockFunnel({ entity: { id: 'funnel-2' } });

                const { context, getFunnelElements, cleanup } = await setup({
                    funnels: [funnel1, funnel2]
                });

                expect(getFunnelElements().size()).toBe(2);

                // Re-render with only funnel1
                context.funnels = [funnel1];
                FunnelRenderer.render(context);

                expect(getFunnelElements().size()).toBe(1);
                expect(getFunnelElements().attr('id')).toBe('id-funnel-1');

                cleanup();
            });

            it('should remove all funnels when data is empty', async () => {
                const funnel = createMockFunnel();
                const { context, getFunnelElements, cleanup } = await setup({
                    funnels: [funnel]
                });

                expect(getFunnelElements().size()).toBe(1);

                // Re-render with empty array
                context.funnels = [];
                FunnelRenderer.render(context);

                expect(getFunnelElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const funnel = createMockFunnel();
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const funnelEl = getFunnelElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            funnelEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(funnel, expect.any(MouseEvent));

            cleanup();
        });

        it('should not attach click handler when canSelect is false', async () => {
            const onClick = vi.fn();
            const funnel = createMockFunnel();
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                canSelect: false,
                callbacks: { onClick }
            });

            // Simulate mousedown event
            const funnelEl = getFunnelElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            funnelEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should ignore right-click events', async () => {
            const onClick = vi.fn();
            const funnel = createMockFunnel();
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                canSelect: true,
                callbacks: { onClick }
            });

            // Simulate right-click (button: 2)
            const funnelEl = getFunnelElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            funnelEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });

        it('should attach double-click handler when onDoubleClick callback provided', async () => {
            const onDoubleClick = vi.fn();
            const funnel = createMockFunnel();
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                canSelect: true,
                callbacks: { onDoubleClick }
            });

            // Simulate dblclick event
            const funnelEl = getFunnelElements().node()!;
            const event = new MouseEvent('dblclick', { bubbles: true });
            funnelEl.dispatchEvent(event);

            expect(onDoubleClick).toHaveBeenCalledTimes(1);
            expect(onDoubleClick).toHaveBeenCalledWith(funnel, expect.any(MouseEvent));

            cleanup();
        });
    });

    describe('pan', () => {
        it('should update funnel elements during pan', async () => {
            const funnel = createMockFunnel({
                entity: { position: { x: 500, y: 600 } }
            });
            const { context, getFunnelElements, cleanup } = await setup({
                funnels: [funnel]
            });

            // Mark as entering (becoming visible)
            getFunnelElements().classed('entering', true).classed('visible', true);

            // Call pan to update
            FunnelRenderer.pan(getFunnelElements(), context);

            // Funnel should still have correct transform
            expect(getFunnelElements().attr('transform')).toBe('translate(500, 600)');

            cleanup();
        });

        it('should update cursor during pan based on canSelect', async () => {
            const funnel = createMockFunnel();
            const { context, getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                canSelect: true
            });

            // Call pan
            FunnelRenderer.pan(getFunnelElements(), context);

            expect(getFunnelElements().style('cursor')).toBe('pointer');

            cleanup();
        });
    });

    describe('data join behavior', () => {
        it('should update existing funnels without recreating them', async () => {
            const funnel = createMockFunnel({
                entity: { id: 'funnel-1', position: { x: 100, y: 100 } }
            });
            const { context, getFunnelElements, cleanup } = await setup({
                funnels: [funnel]
            });

            // Get reference to the DOM element
            const originalElement = getFunnelElements().node();

            // Update position and re-render
            funnel.entity.position = { x: 200, y: 200 };
            context.funnels = [funnel];
            FunnelRenderer.render(context);

            // Should be the same DOM element
            expect(getFunnelElements().node()).toBe(originalElement);
            // But with updated transform
            expect(getFunnelElements().attr('transform')).toBe('translate(200, 200)');

            cleanup();
        });

        it('should add new funnels while keeping existing ones', async () => {
            const funnel1 = createMockFunnel({ entity: { id: 'funnel-1' } });
            const { context, getFunnelElements, cleanup } = await setup({
                funnels: [funnel1]
            });

            expect(getFunnelElements().size()).toBe(1);

            // Add a second funnel
            const funnel2 = createMockFunnel({ entity: { id: 'funnel-2' } });
            context.funnels = [funnel1, funnel2];
            FunnelRenderer.render(context);

            expect(getFunnelElements().size()).toBe(2);

            cleanup();
        });
    });

    describe('disabled state', () => {
        it('should not be disabled by default', async () => {
            const funnel = createMockFunnel();
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel]
            });

            const funnelEl = getFunnelElements();
            expect(funnelEl.classed('disabled')).toBe(false);
            expect(funnelEl.style('opacity')).toBe('');
            expect(funnelEl.style('cursor')).toBe('pointer');

            cleanup();
        });

        it('should apply disabled state when funnel ID is in disabledFunnelIds', async () => {
            const funnel = createMockFunnel({ entity: { id: 'funnel-1' } });
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                disabledFunnelIds: new Set(['funnel-1'])
            });

            const funnelEl = getFunnelElements();
            expect(funnelEl.classed('disabled')).toBe(true);
            expect(funnelEl.style('opacity')).toBe('0.6');
            expect(funnelEl.style('cursor')).toBe('not-allowed');

            cleanup();
        });

        it('should not apply disabled state when funnel ID is not in disabledFunnelIds', async () => {
            const funnel = createMockFunnel({ entity: { id: 'funnel-1' } });
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel],
                disabledFunnelIds: new Set(['funnel-2', 'funnel-3']) // Different IDs
            });

            const funnelEl = getFunnelElements();
            expect(funnelEl.classed('disabled')).toBe(false);

            cleanup();
        });

        it('should update disabled state on re-render', async () => {
            const funnel = createMockFunnel({ entity: { id: 'funnel-1' } });
            const { context, getFunnelElements, cleanup } = await setup({
                funnels: [funnel]
            });

            // Initially not disabled
            expect(getFunnelElements().classed('disabled')).toBe(false);

            // Add to disabled set and re-render
            context.disabledFunnelIds = new Set(['funnel-1']);
            FunnelRenderer.render(context);

            expect(getFunnelElements().classed('disabled')).toBe(true);

            // Remove from disabled set and re-render
            context.disabledFunnelIds = new Set();
            FunnelRenderer.render(context);

            expect(getFunnelElements().classed('disabled')).toBe(false);

            cleanup();
        });
    });

    describe('authorization styling', () => {
        it('should apply unauthorized class to border and body when canRead is false', async () => {
            const funnel = createMockFunnel({
                entity: {
                    permissions: { canRead: false, canWrite: false }
                }
            });
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel]
            });

            expect(getFunnelElements().select('rect.border').classed('unauthorized')).toBe(true);
            expect(getFunnelElements().select('rect.body').classed('unauthorized')).toBe(true);

            cleanup();
        });

        it('should not apply unauthorized class when canRead is true', async () => {
            const funnel = createMockFunnel({
                entity: {
                    permissions: { canRead: true, canWrite: false }
                }
            });
            const { getFunnelElements, cleanup } = await setup({
                funnels: [funnel]
            });

            expect(getFunnelElements().select('rect.border').classed('unauthorized')).toBe(false);
            expect(getFunnelElements().select('rect.body').classed('unauthorized')).toBe(false);

            cleanup();
        });
    });
});
