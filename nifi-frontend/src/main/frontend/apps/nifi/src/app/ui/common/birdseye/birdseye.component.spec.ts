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

import { TestBed } from '@angular/core/testing';
import { ComponentType } from '@nifi/shared';
import { CanvasBirdseyeComponent } from './birdseye.component';
import { BirdseyeComponentData, BirdseyeTransform } from './birdseye.types';
import { Dimension } from '../canvas/canvas.types';

function createMockComponent(
    options: {
        id?: string;
        type?: ComponentType;
        x?: number;
        y?: number;
        width?: number;
        height?: number;
        fillColor?: string;
    } = {}
): BirdseyeComponentData {
    return {
        id: options.id || `component-${Math.random().toString(36).substring(2, 11)}`,
        type: options.type || ComponentType.Processor,
        position: {
            x: options.x ?? 100,
            y: options.y ?? 100
        },
        dimensions: {
            width: options.width ?? 352,
            height: options.height ?? 128
        },
        ...(options.fillColor !== undefined ? { fillColor: options.fillColor } : {})
    };
}

function createMockTransform(
    options: {
        translateX?: number;
        translateY?: number;
        scale?: number;
    } = {}
): BirdseyeTransform {
    return {
        translate: {
            x: options.translateX ?? 0,
            y: options.translateY ?? 0
        },
        scale: options.scale ?? 1
    };
}

function createMockDimensions(
    options: {
        width?: number;
        height?: number;
    } = {}
): Dimension {
    return {
        width: options.width ?? 1000,
        height: options.height ?? 800
    };
}

interface SetupOptions {
    components?: BirdseyeComponentData[];
    transform?: BirdseyeTransform;
    canvasDimensions?: Dimension;
}

async function setup(options: SetupOptions = {}) {
    await TestBed.configureTestingModule({
        imports: [CanvasBirdseyeComponent]
    }).compileComponents();

    const fixture = TestBed.createComponent(CanvasBirdseyeComponent);
    const component = fixture.componentInstance;

    fixture.componentRef.setInput('components', options.components ?? []);
    fixture.componentRef.setInput(
        'transform',
        options.transform ?? {
            translate: { x: 0, y: 0 },
            scale: 1
        }
    );
    fixture.componentRef.setInput('canvasDimensions', options.canvasDimensions ?? { width: 1000, height: 800 });

    fixture.detectChanges();

    const containerElement = fixture.nativeElement.querySelector('.birdseye-container');
    const canvasElement = fixture.nativeElement.querySelector('.birdseye-canvas');
    const svgElement = fixture.nativeElement.querySelector('.birdseye-svg');
    const brushElement = fixture.nativeElement.querySelector('.birdseye-brush');

    return {
        fixture,
        component,
        containerElement,
        canvasElement,
        svgElement,
        brushElement
    };
}

describe('CanvasBirdseyeComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should render canvas element for component visualization', async () => {
            const { canvasElement } = await setup();
            expect(canvasElement).toBeTruthy();
            expect(canvasElement.tagName.toLowerCase()).toBe('canvas');
        });

        it('should render SVG element for interactive brush', async () => {
            const { svgElement } = await setup();
            expect(svgElement).toBeTruthy();
            expect(svgElement.tagName.toLowerCase()).toBe('svg');
        });

        it('should render viewport brush element', async () => {
            const components = [createMockComponent()];
            const { brushElement } = await setup({ components });
            expect(brushElement).toBeTruthy();
            expect(brushElement.tagName.toLowerCase()).toBe('rect');
        });

        it('should set default canvas style dimensions', async () => {
            const { canvasElement } = await setup();
            expect(canvasElement.style.width).toBe('200px');
            expect(canvasElement.style.height).toBe('150px');
        });

        it('should set default SVG dimensions', async () => {
            const { svgElement } = await setup();
            expect(svgElement.getAttribute('width')).toBe('200');
            expect(svgElement.getAttribute('height')).toBe('150');
        });
    });

    describe('Component rendering', () => {
        it('should render with components present', async () => {
            const components = [
                createMockComponent({ id: 'proc-1', type: ComponentType.Processor, x: 100, y: 100 }),
                createMockComponent({ id: 'proc-2', type: ComponentType.Processor, x: 500, y: 300 })
            ];
            const { canvasElement } = await setup({ components });
            expect(canvasElement).toBeTruthy();
        });

        it('should handle empty components array', async () => {
            const { canvasElement, component } = await setup({ components: [] });
            expect(canvasElement).toBeTruthy();
            expect(component).toBeTruthy();
        });

        it('should accept all supported component types without error', async () => {
            const componentTypes: ComponentType[] = [
                ComponentType.Processor,
                ComponentType.ProcessGroup,
                ComponentType.RemoteProcessGroup,
                ComponentType.InputPort,
                ComponentType.OutputPort,
                ComponentType.Funnel,
                ComponentType.Label,
                ComponentType.Connection
            ];

            const components = componentTypes.map((type, index) => createMockComponent({ type, x: index * 200, y: 0 }));
            const { component } = await setup({ components });
            expect(component).toBeTruthy();
        });
    });

    describe('Viewport brush', () => {
        it('should position brush via a transform attribute', async () => {
            const components = [createMockComponent({ x: 0, y: 0 })];
            const transform = createMockTransform({ translateX: -100, translateY: -50, scale: 1 });
            const { brushElement } = await setup({ components, transform });

            const transformAttr = brushElement.getAttribute('transform');
            expect(transformAttr).toContain('translate');
        });

        it('should size brush based on canvas dimensions and scale', async () => {
            const components = [createMockComponent({ x: 0, y: 0, width: 2000, height: 1500 })];
            const transform = createMockTransform({ scale: 0.5 });
            const canvasDimensions = createMockDimensions({ width: 1000, height: 800 });
            const { brushElement } = await setup({ components, transform, canvasDimensions });

            const width = brushElement.getAttribute('width');
            const height = brushElement.getAttribute('height');
            expect(parseFloat(width!)).toBeGreaterThan(0);
            expect(parseFloat(height!)).toBeGreaterThan(0);
        });

        it('should enforce minimum brush size of 10', async () => {
            const components = [createMockComponent({ x: 0, y: 0, width: 10000, height: 10000 })];
            const transform = createMockTransform({ scale: 0.1 });
            const canvasDimensions = createMockDimensions({ width: 100, height: 80 });
            const { brushElement } = await setup({ components, transform, canvasDimensions });

            const width = parseFloat(brushElement.getAttribute('width')!);
            const height = parseFloat(brushElement.getAttribute('height')!);
            expect(width).toBeGreaterThanOrEqual(10);
            expect(height).toBeGreaterThanOrEqual(10);
        });

        it('should reflect a panned viewport in the brush transform', async () => {
            const components = [createMockComponent({ x: 0, y: 0 }), createMockComponent({ x: 1000, y: 800 })];
            const transform = createMockTransform({ translateX: -500, translateY: -400, scale: 1 });
            const { brushElement } = await setup({ components, transform });

            const transformAttr = brushElement.getAttribute('transform');
            expect(transformAttr).toContain('translate');
            expect(transformAttr).not.toBe('translate(0, 0)');
        });
    });

    describe('Bounds calculation', () => {
        it('should accept components at extreme coordinates', async () => {
            const components = [
                createMockComponent({ x: -500, y: -300, width: 100, height: 50 }),
                createMockComponent({ x: 1000, y: 800, width: 100, height: 50 })
            ];
            const { component } = await setup({ components });
            expect(component).toBeTruthy();
        });

        it('should accept a viewport panned far from any component', async () => {
            const components = [createMockComponent({ x: 0, y: 0 })];
            const transform = createMockTransform({ translateX: -5000, translateY: -3000 });
            const canvasDimensions = createMockDimensions({ width: 1000, height: 800 });
            const { component } = await setup({ components, transform, canvasDimensions });
            expect(component).toBeTruthy();
        });
    });

    describe('Output events', () => {
        it('should expose viewportChange', async () => {
            const { component } = await setup({ components: [createMockComponent()] });
            expect(component.viewportChange).toBeDefined();
        });

        it('should expose dragStart', async () => {
            const { component } = await setup({ components: [createMockComponent()] });
            expect(component.dragStart).toBeDefined();
        });

        it('should expose dragEnd', async () => {
            const { component } = await setup({ components: [createMockComponent()] });
            expect(component.dragEnd).toBeDefined();
        });
    });

    describe('Component palette', () => {
        // The default test environment does not implement CanvasRenderingContext2D, so the
        // component's rendering code path is normally a no-op. To exercise the paint logic we
        // install a stub 2D context on HTMLCanvasElement that records every value assigned to
        // fillStyle and strokeStyle while satisfying the methods the component invokes during
        // initializeBirdseye(), renderComponents(), and updateBirdseyeSize().
        function installPaintTracker() {
            const fillColors: string[] = [];
            const strokeColors: string[] = [];

            const ctx: any = {
                _fillStyle: '',
                _strokeStyle: '',
                set fillStyle(value: string) {
                    fillColors.push(String(value));
                    this._fillStyle = value;
                },
                get fillStyle() {
                    return this._fillStyle;
                },
                set strokeStyle(value: string) {
                    strokeColors.push(String(value));
                    this._strokeStyle = value;
                },
                get strokeStyle() {
                    return this._strokeStyle;
                },
                scale: vi.fn(),
                clearRect: vi.fn(),
                save: vi.fn(),
                restore: vi.fn(),
                translate: vi.fn(),
                fillRect: vi.fn(),
                strokeRect: vi.fn(),
                setTransform: vi.fn()
            };

            const spy = vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(ctx as any);

            return {
                fillColors,
                strokeColors,
                restore: () => spy.mockRestore()
            };
        }

        it('should use the flow-designer palette for processors with a contrast stroke', async () => {
            const tracker = installPaintTracker();
            try {
                const components = [createMockComponent({ type: ComponentType.Processor })];
                const { fixture } = await setup({ components });
                await fixture.whenStable();

                expect(tracker.fillColors).toContain('#dde4eb');
                expect(tracker.strokeColors).toContain('#000000');
            } finally {
                tracker.restore();
            }
        });

        it('should default labels to the flow-designer label fill', async () => {
            const tracker = installPaintTracker();
            try {
                const components = [createMockComponent({ type: ComponentType.Label })];
                const { fixture } = await setup({ components });
                await fixture.whenStable();

                expect(tracker.fillColors).toContain('#fff7d7');
            } finally {
                tracker.restore();
            }
        });

        it('should honor a user-configured fillColor override', async () => {
            const tracker = installPaintTracker();
            try {
                const components = [createMockComponent({ type: ComponentType.Processor, fillColor: '#123456' })];
                const { fixture } = await setup({ components });
                await fixture.whenStable();

                expect(tracker.fillColors).toContain('#123456');
                // #123456 is dark, so the contrast-derived stroke should be white.
                expect(tracker.strokeColors).toContain('#ffffff');
            } finally {
                tracker.restore();
            }
        });

        it('should derive a black stroke for light user-configured fills', async () => {
            const tracker = installPaintTracker();
            try {
                const components = [createMockComponent({ type: ComponentType.Label, fillColor: '#fefefe' })];
                const { fixture } = await setup({ components });
                await fixture.whenStable();

                expect(tracker.fillColors).toContain('#fefefe');
                expect(tracker.strokeColors).toContain('#000000');
            } finally {
                tracker.restore();
            }
        });
    });

    describe('Cleanup', () => {
        it('should not throw when destroyed after rendering', async () => {
            const { fixture } = await setup({ components: [createMockComponent()] });
            expect(() => fixture.destroy()).not.toThrow();
        });
    });

    describe('Edge cases', () => {
        it('should handle a single component', async () => {
            const { component } = await setup({ components: [createMockComponent()] });
            expect(component).toBeTruthy();
        });

        it('should handle components at negative coordinates', async () => {
            const components = [createMockComponent({ x: -1000, y: -500 }), createMockComponent({ x: -500, y: -250 })];
            const { component } = await setup({ components });
            expect(component).toBeTruthy();
        });

        it('should handle very small scale', async () => {
            const components = [createMockComponent()];
            const transform = createMockTransform({ scale: 0.1 });
            const { component } = await setup({ components, transform });
            expect(component).toBeTruthy();
        });

        it('should handle very large scale', async () => {
            const components = [createMockComponent()];
            const transform = createMockTransform({ scale: 8 });
            const { component } = await setup({ components, transform });
            expect(component).toBeTruthy();
        });

        it('should handle zero-size canvas dimensions gracefully', async () => {
            const components = [createMockComponent()];
            const canvasDimensions = createMockDimensions({ width: 0, height: 0 });
            const { component } = await setup({ components, canvasDimensions });
            expect(component).toBeTruthy();
        });
    });
});
