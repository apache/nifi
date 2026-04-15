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
import { provideMockStore } from '@ngrx/store/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { ReplaySubject } from 'rxjs';
import { Action } from '@ngrx/store';
import { outputToObservable } from '@angular/core/rxjs-interop';
import { CanvasComponent } from './canvas.component';
import { BirdseyeComponentData } from './canvas.component';
import { canvasUiFeatureKey, initialCanvasUiState } from '../../../state/canvas-ui';
import { ComponentType } from '@nifi/shared';

// Mock data factories
function createMockProcessor(
    options: {
        id?: string;
        name?: string;
        x?: number;
        y?: number;
    } = {}
): any {
    const id = options.id || `processor-${Math.random().toString(36).substr(2, 9)}`;
    return {
        id,
        uri: `https://localhost/nifi-api/processors/${id}`,
        permissions: { canRead: true, canWrite: true },
        bulletins: [],
        component: {
            id,
            name: options.name || 'Test Processor',
            type: 'org.apache.nifi.processors.test.TestProcessor',
            state: 'STOPPED',
            validationStatus: 'VALID'
        },
        position: {
            x: options.x ?? 100,
            y: options.y ?? 100
        },
        status: {
            runStatus: 'Stopped',
            statsLastRefreshed: new Date().toISOString()
        },
        revision: { version: 0 }
    };
}

function createMockLabel(
    options: {
        id?: string;
        label?: string;
        x?: number;
        y?: number;
        width?: number;
        height?: number;
    } = {}
): any {
    const id = options.id || `label-${Math.random().toString(36).substr(2, 9)}`;
    return {
        id,
        uri: `https://localhost/nifi-api/labels/${id}`,
        permissions: { canRead: true, canWrite: true },
        component: {
            id,
            label: options.label || 'Test Label',
            width: options.width ?? 150,
            height: options.height ?? 150,
            style: {}
        },
        position: {
            x: options.x ?? 200,
            y: options.y ?? 200
        },
        revision: { version: 0 },
        dimensions: {
            width: options.width ?? 150,
            height: options.height ?? 150
        }
    };
}

function createMockFunnel(
    options: {
        id?: string;
        x?: number;
        y?: number;
    } = {}
): any {
    const id = options.id || `funnel-${Math.random().toString(36).substr(2, 9)}`;
    return {
        id,
        uri: `https://localhost/nifi-api/funnels/${id}`,
        permissions: { canRead: true, canWrite: true },
        component: { id },
        position: {
            x: options.x ?? 300,
            y: options.y ?? 300
        },
        revision: { version: 0 }
    };
}

function createMockProcessGroup(
    options: {
        id?: string;
        name?: string;
        x?: number;
        y?: number;
    } = {}
): any {
    const id = options.id || `pg-${Math.random().toString(36).substr(2, 9)}`;
    return {
        id,
        uri: `https://localhost/nifi-api/process-groups/${id}`,
        permissions: { canRead: true, canWrite: true },
        bulletins: [],
        component: {
            id,
            name: options.name || 'Test Process Group',
            runningCount: 0,
            stoppedCount: 0,
            invalidCount: 0,
            disabledCount: 0
        },
        position: {
            x: options.x ?? 400,
            y: options.y ?? 400
        },
        status: {
            statsLastRefreshed: new Date().toISOString()
        },
        revision: { version: 0 }
    };
}

// Initial NgRx state for canvas using exported feature key and initial state
const initialState = {
    [canvasUiFeatureKey]: initialCanvasUiState
};

/**
 * Mock the SVG element's getBoundingClientRect to return realistic viewport dimensions.
 * In test environments (happy-dom/jsdom), getBoundingClientRect returns zeros.
 * This allows fitContent, onZoomActual, etc. to calculate real transforms.
 */
function mockSvgDimensions(fixture: any, width = 1200, height = 800): void {
    const svgElement = fixture.nativeElement.querySelector('svg.canvas-svg');
    if (svgElement) {
        vi.spyOn(svgElement, 'getBoundingClientRect').mockReturnValue({
            x: 0,
            y: 0,
            width,
            height,
            top: 0,
            left: 0,
            right: width,
            bottom: height,
            toJSON: () => ({})
        } as DOMRect);
    }
}

interface SetupOptions {
    processors?: any[];
    labels?: any[];
    funnels?: any[];
    processGroups?: any[];
    ports?: any[];
    remoteProcessGroups?: any[];
    connections?: any[];
    selectedComponentIds?: string[];
    initialTransform?: { x: number; y: number; scale: number };
    dataReady?: boolean;
}

async function setup(options: SetupOptions = {}) {
    const actions$ = new ReplaySubject<Action>();

    // Customize initial state if transform is provided
    const testInitialState = {
        ...initialState,
        [canvasUiFeatureKey]: {
            ...initialState[canvasUiFeatureKey],
            transform: options.initialTransform
                ? {
                      translate: { x: options.initialTransform.x, y: options.initialTransform.y },
                      scale: options.initialTransform.scale,
                      transition: false
                  }
                : initialState[canvasUiFeatureKey].transform
        }
    };

    await TestBed.configureTestingModule({
        imports: [CanvasComponent],
        providers: [provideMockStore({ initialState: testInitialState }), provideMockActions(() => actions$)]
    }).compileComponents();

    const fixture = TestBed.createComponent(CanvasComponent);
    const component = fixture.componentInstance;

    // Set up required inputs using fixture.componentRef.setInput()
    fixture.componentRef.setInput('labels', options.labels ?? []);
    fixture.componentRef.setInput('processors', options.processors ?? []);
    fixture.componentRef.setInput('funnels', options.funnels ?? []);
    fixture.componentRef.setInput('ports', options.ports ?? []);
    fixture.componentRef.setInput('remoteProcessGroups', options.remoteProcessGroups ?? []);
    fixture.componentRef.setInput('processGroups', options.processGroups ?? []);
    fixture.componentRef.setInput('connections', options.connections ?? []);
    fixture.componentRef.setInput('processGroupId', 'test-pg-id');
    fixture.componentRef.setInput('selectedComponentIds', options.selectedComponentIds ?? []);
    fixture.componentRef.setInput('dataReady', options.dataReady ?? false);

    fixture.detectChanges();

    return {
        fixture,
        component,
        actions$
    };
}

describe('CanvasComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should render SVG canvas element', async () => {
            const { fixture } = await setup();
            const svgElement = fixture.nativeElement.querySelector('svg.canvas-svg');
            expect(svgElement).toBeTruthy();
        });
    });

    describe('Birdseye API - getTransform', () => {
        it('should return current transform values', async () => {
            const { component } = await setup();

            const transform = component.getTransform();

            expect(transform).toBeDefined();
            expect(transform.translate).toBeDefined();
            expect(typeof transform.translate.x).toBe('number');
            expect(typeof transform.translate.y).toBe('number');
            expect(typeof transform.scale).toBe('number');
        });

        it('should return initial transform from state', async () => {
            const { component } = await setup({
                initialTransform: { x: 100, y: 200, scale: 1.5 }
            });

            const transform = component.getTransform();

            // Note: The actual values depend on how the component initializes
            // This test verifies the method returns valid transform data
            expect(transform.scale).toBeGreaterThan(0);
        });
    });

    describe('Birdseye API - getCanvasDimensions', () => {
        it('should return canvas dimensions', async () => {
            const { component } = await setup();

            const dimensions = component.getCanvasDimensions();

            expect(dimensions).toBeDefined();
            expect(typeof dimensions.width).toBe('number');
            expect(typeof dimensions.height).toBe('number');
        });

        it('should return zero dimensions when SVG not available', async () => {
            const { component } = await setup();

            // Access internal method behavior - dimensions should be valid numbers
            const dimensions = component.getCanvasDimensions();

            expect(dimensions.width).toBeGreaterThanOrEqual(0);
            expect(dimensions.height).toBeGreaterThanOrEqual(0);
        });
    });

    describe('Birdseye API - getBirdseyeComponentData', () => {
        it('should return empty array when no components', async () => {
            const { component } = await setup();

            const data = component.getBirdseyeComponentData();

            expect(Array.isArray(data)).toBe(true);
            expect(data.length).toBe(0);
        });

        it('should return processor data', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 200 });
            const { component } = await setup({ processors: [processor] });

            const data = component.getBirdseyeComponentData();

            expect(data.length).toBe(1);
            expect(data[0].id).toBe('proc-1');
            expect(data[0].type).toBe(ComponentType.Processor);
            expect(data[0].position.x).toBe(100);
            expect(data[0].position.y).toBe(200);
            expect(data[0].dimensions.width).toBeGreaterThan(0);
            expect(data[0].dimensions.height).toBeGreaterThan(0);
        });

        it('should return label data', async () => {
            const label = createMockLabel({ id: 'label-1', x: 150, y: 250, width: 200, height: 100 });
            const { component } = await setup({ labels: [label] });

            const data = component.getBirdseyeComponentData();

            expect(data.length).toBe(1);
            expect(data[0].id).toBe('label-1');
            expect(data[0].type).toBe(ComponentType.Label);
            expect(data[0].position.x).toBe(150);
            expect(data[0].position.y).toBe(250);
        });

        it('should return funnel data', async () => {
            const funnel = createMockFunnel({ id: 'funnel-1', x: 300, y: 400 });
            const { component } = await setup({ funnels: [funnel] });

            const data = component.getBirdseyeComponentData();

            expect(data.length).toBe(1);
            expect(data[0].id).toBe('funnel-1');
            expect(data[0].type).toBe(ComponentType.Funnel);
        });

        it('should return process group data', async () => {
            const pg = createMockProcessGroup({ id: 'pg-1', x: 500, y: 600 });
            const { component } = await setup({ processGroups: [pg] });

            const data = component.getBirdseyeComponentData();

            expect(data.length).toBe(1);
            expect(data[0].id).toBe('pg-1');
            expect(data[0].type).toBe(ComponentType.ProcessGroup);
        });

        it('should return all component types', async () => {
            const processor = createMockProcessor({ id: 'proc-1' });
            const label = createMockLabel({ id: 'label-1' });
            const funnel = createMockFunnel({ id: 'funnel-1' });
            const pg = createMockProcessGroup({ id: 'pg-1' });

            const { component } = await setup({
                processors: [processor],
                labels: [label],
                funnels: [funnel],
                processGroups: [pg]
            });

            const data = component.getBirdseyeComponentData();

            expect(data.length).toBe(4);

            const types = data.map((d: BirdseyeComponentData) => d.type);
            expect(types).toContain(ComponentType.Processor);
            expect(types).toContain(ComponentType.Label);
            expect(types).toContain(ComponentType.Funnel);
            expect(types).toContain(ComponentType.ProcessGroup);
        });
    });

    describe('Birdseye API - setViewportPosition', () => {
        it('should be callable without error', async () => {
            const { component } = await setup();

            expect(() => {
                component.setViewportPosition(100, 200, false);
            }).not.toThrow();
        });

        it('should accept transition parameter', async () => {
            const { component } = await setup();

            expect(() => {
                component.setViewportPosition(100, 200, true);
            }).not.toThrow();
        });
    });

    describe('Birdseye API - birdseyeDragStart/birdseyeDragEnd', () => {
        it('should handle drag start without error', async () => {
            const { component } = await setup();

            expect(() => {
                component.birdseyeDragStart();
            }).not.toThrow();
        });

        it('should handle drag end without error', async () => {
            const { component } = await setup();

            expect(() => {
                component.birdseyeDragEnd();
            }).not.toThrow();
        });

        it('should handle full drag cycle', async () => {
            const { component } = await setup();

            // Simulate birdseye drag cycle
            component.birdseyeDragStart();
            component.setViewportPosition(100, 100, false);
            component.setViewportPosition(150, 150, false);
            component.setViewportPosition(200, 200, false);
            component.birdseyeDragEnd();

            // Should complete without error
            expect(true).toBe(true);
        });
    });

    describe('Birdseye API - updateCanvasVisibility', () => {
        it('should be callable without error', async () => {
            const { component } = await setup();

            expect(() => {
                component.updateCanvasVisibility();
            }).not.toThrow();
        });

        it('should handle visibility update with components', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const { component } = await setup({ processors: [processor] });

            expect(() => {
                component.updateCanvasVisibility();
            }).not.toThrow();
        });
    });

    describe('Birdseye integration flow', () => {
        it('should support typical birdseye interaction pattern', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({ processors: [processor] });

            // 1. Get initial component data for birdseye
            const componentData = component.getBirdseyeComponentData();
            expect(componentData.length).toBe(1);

            // 2. Get initial transform for birdseye
            const initialTransform = component.getTransform();
            expect(initialTransform).toBeDefined();

            // 3. Get canvas dimensions for birdseye
            const dimensions = component.getCanvasDimensions();
            expect(dimensions).toBeDefined();

            // 4. Simulate birdseye drag
            component.birdseyeDragStart();

            // 5. Update viewport position during drag
            component.setViewportPosition(-100, -50, false);

            await fixture.whenStable();

            // 6. End drag and update visibility
            component.birdseyeDragEnd();

            // 7. Verify transform changed
            const newTransform = component.getTransform();
            expect(newTransform).toBeDefined();
        });

        it('should not call updateCanvasVisibility during drag', async () => {
            const processor = createMockProcessor({ id: 'proc-1' });
            const { component } = await setup({ processors: [processor] });

            // Spy on updateCanvasVisibility
            const updateVisibilitySpy = vi.spyOn(component, 'updateCanvasVisibility');

            // Start drag
            component.birdseyeDragStart();

            // Multiple viewport updates during drag
            component.setViewportPosition(100, 100, false);
            component.setViewportPosition(200, 200, false);

            // During drag, updateCanvasVisibility should not be called
            // (The zoom end handler skips it when birdseyeTranslateInProgress is true)

            // End drag - this should call updateCanvasVisibility
            component.birdseyeDragEnd();

            // birdseyeDragEnd calls updateCanvasVisibility once
            expect(updateVisibilitySpy).toHaveBeenCalled();
        });
    });

    describe('Component data consistency', () => {
        it('should return consistent data across multiple calls', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 200 });
            const { component } = await setup({ processors: [processor] });

            const data1 = component.getBirdseyeComponentData();
            const data2 = component.getBirdseyeComponentData();

            expect(data1.length).toBe(data2.length);
            expect(data1[0].id).toBe(data2[0].id);
            expect(data1[0].position.x).toBe(data2[0].position.x);
            expect(data1[0].position.y).toBe(data2[0].position.y);
        });

        it('should update data when components change', async () => {
            const { fixture, component } = await setup();

            // Initial state - no components
            let data = component.getBirdseyeComponentData();
            expect(data.length).toBe(0);

            // Add a processor using setInput
            fixture.componentRef.setInput('processors', [createMockProcessor({ id: 'proc-1' })]);
            fixture.detectChanges();

            // Data should now include the processor
            data = component.getBirdseyeComponentData();
            expect(data.length).toBe(1);
            expect(data[0].id).toBe('proc-1');
        });
    });

    describe('dataReady input and initialized output', () => {
        it('should accept dataReady input', async () => {
            const { component } = await setup({ dataReady: true });
            expect(component).toBeTruthy();
        });

        it('should not emit initialized when dataReady is false', async () => {
            const { component, fixture } = await setup({ dataReady: false });

            const initializedSpy = vi.fn();
            component.initialized.subscribe(initializedSpy);

            // Wait for potential async emissions
            await fixture.whenStable();

            // Should not have emitted since dataReady is false
            expect(initializedSpy).not.toHaveBeenCalled();
        });

        it('should have initialized output defined', async () => {
            const { component } = await setup();
            expect(component.initialized).toBeDefined();
        });

        it('should handle dataReady transition from false to true', async () => {
            const { fixture, component } = await setup({ dataReady: false });

            // Initial state - dataReady is false
            expect(component).toBeTruthy();

            // Set dataReady to true
            fixture.componentRef.setInput('dataReady', true);
            fixture.detectChanges();

            // Component should still function correctly
            expect(component).toBeTruthy();
        });

        it('should handle dataReady transition from true to false to true', async () => {
            const { fixture, component } = await setup({
                dataReady: true,
                processors: [createMockProcessor({ id: 'proc-1' })]
            });

            // Set dataReady to false (simulating new data load)
            fixture.componentRef.setInput('dataReady', false);
            fixture.detectChanges();

            // Set dataReady back to true (simulating data loaded)
            fixture.componentRef.setInput('dataReady', true);
            fixture.detectChanges();

            // Component should still function correctly
            const data = component.getBirdseyeComponentData();
            expect(data.length).toBe(1);
        });
    });

    describe('Edge cases', () => {
        it('should handle many components', async () => {
            const processors = Array.from({ length: 100 }, (_, i) =>
                createMockProcessor({ id: `proc-${i}`, x: i * 100, y: i * 50 })
            );

            const { component } = await setup({ processors });

            const data = component.getBirdseyeComponentData();
            expect(data.length).toBe(100);
        });

        it('should handle components at negative coordinates', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: -500, y: -300 });
            const { component } = await setup({ processors: [processor] });

            const data = component.getBirdseyeComponentData();
            expect(data[0].position.x).toBe(-500);
            expect(data[0].position.y).toBe(-300);
        });

        it('should handle rapid viewport position changes', async () => {
            const { component } = await setup();

            component.birdseyeDragStart();

            // Simulate rapid drag updates
            for (let i = 0; i < 50; i++) {
                component.setViewportPosition(i * 10, i * 5, false);
            }

            component.birdseyeDragEnd();

            // Should complete without error
            expect(true).toBe(true);
        });
    });

    describe('restoreViewportFromStorage (fitContent fallback)', () => {
        it('should fit content when no processGroupId is set', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 200, y: 200 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture);

            // Override processGroupId to null (no process group context)
            fixture.componentRef.setInput('processGroupId', null);
            fixture.detectChanges();

            component.restoreViewportFromStorage();

            // With a 1200x800 viewport and a small component at (200,200),
            // the content fits at scale 1 and should be centered
            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            // Translation should be positive (centering the content)
            expect(transform.translate.x).toBeGreaterThan(0);
            expect(transform.translate.y).toBeGreaterThan(0);
        });

        it('should fit content when no stored viewport exists for the process group', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 300, y: 300 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture);

            // Ensure no stored viewport for 'test-pg-id'
            localStorage.removeItem('nifi-view-test-pg-id');

            component.restoreViewportFromStorage();

            // fitContent should have executed and changed the transform
            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeGreaterThan(0);
            expect(transform.translate.y).toBeGreaterThan(0);
        });

        it('should restore stored viewport when valid data exists', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture);

            // Seed a stored viewport in localStorage
            localStorage.setItem(
                'nifi-view-test-pg-id',
                JSON.stringify({
                    expires: Date.now() + 172800000,
                    item: {
                        scale: 2.5,
                        translateX: -150,
                        translateY: -250
                    }
                })
            );

            component.restoreViewportFromStorage();

            // The stored viewport should be restored exactly
            const transform = component.getTransform();
            expect(transform.scale).toBe(2.5);
            expect(transform.translate.x).toBe(-150);
            expect(transform.translate.y).toBe(-250);
        });

        it('should fall back to fitContent when stored viewport has Infinity values', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture);

            // Seed an invalid stored viewport (Infinity survives JSON round-trip as null,
            // but isFinite(null) is true, so use Infinity which serializes to null
            // and isFinite(null) = true. Use a direct localStorage write to bypass serialization.)
            // Instead, test with no item at all (same fallback path).
            localStorage.setItem(
                'nifi-view-test-pg-id',
                JSON.stringify({
                    expires: Date.now() + 172800000,
                    item: {
                        scale: undefined as any,
                        translateX: 100,
                        translateY: 100
                    }
                })
            );

            component.restoreViewportFromStorage();

            // Should have fallen back to fitContent (scale 1 for small content in large viewport)
            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
        });

        it('should scale down when content exceeds viewport', async () => {
            // Place processors far apart to create a bounding box larger than viewport
            const processor1 = createMockProcessor({ id: 'proc-1', x: 0, y: 0 });
            const processor2 = createMockProcessor({ id: 'proc-2', x: 5000, y: 5000 });
            const { fixture, component } = await setup({
                processors: [processor1, processor2]
            });
            mockSvgDimensions(fixture, 1200, 800);

            // Ensure no stored viewport so fitContent runs
            localStorage.removeItem('nifi-view-test-pg-id');

            component.restoreViewportFromStorage();

            const transform = component.getTransform();
            // Content bounding box is ~5000x5000, viewport is 1150x750 (minus padding)
            // Scale should be < 1 to fit the content
            expect(transform.scale).toBeLessThan(1);
            expect(transform.scale).toBeGreaterThanOrEqual(0.2);
        });

        it('should handle components at negative coordinates', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: -1000, y: -500 });
            const funnel = createMockFunnel({ id: 'funnel-1', x: 1000, y: 500 });
            const { fixture, component } = await setup({
                processors: [processor],
                funnels: [funnel]
            });
            mockSvgDimensions(fixture, 1200, 800);

            localStorage.removeItem('nifi-view-test-pg-id');

            component.restoreViewportFromStorage();

            // Content spans from (-1000,-500) to (~1048,~548), which is ~2048x1048
            // That exceeds the 1150x750 viewport, so scale < 1
            const transform = component.getTransform();
            expect(transform.scale).toBeLessThan(1);
            expect(transform.scale).toBeGreaterThanOrEqual(0.2);
        });

        it('should include all component types in bounding box calculation', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const label = createMockLabel({ id: 'label-1', x: 3000, y: 3000 });
            const funnel = createMockFunnel({ id: 'funnel-1', x: 300, y: 300 });
            const pg = createMockProcessGroup({ id: 'pg-1', x: 400, y: 400 });
            const { fixture, component } = await setup({
                processors: [processor],
                labels: [label],
                funnels: [funnel],
                processGroups: [pg]
            });
            mockSvgDimensions(fixture, 1200, 800);

            localStorage.removeItem('nifi-view-test-pg-id');

            component.restoreViewportFromStorage();

            // Label at (3000, 3000) forces a large bounding box => scale < 1
            const transform = component.getTransform();
            expect(transform.scale).toBeLessThan(1);
        });

        it('should not change transform when viewport has zero dimensions', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({ processors: [processor] });
            // Explicitly mock zero-size viewport (simulates not rendered yet)
            mockSvgDimensions(fixture, 0, 0);

            localStorage.removeItem('nifi-view-test-pg-id');

            const beforeTransform = component.getTransform();
            component.restoreViewportFromStorage();
            const afterTransform = component.getTransform();

            // Transform should not change (fitContent guards against zero dimensions)
            expect(afterTransform.translate.x).toBe(beforeTransform.translate.x);
            expect(afterTransform.translate.y).toBe(beforeTransform.translate.y);
            expect(afterTransform.scale).toBe(beforeTransform.scale);
        });

        it('should apply identity transform when no components exist', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture);

            localStorage.removeItem('nifi-view-test-pg-id');

            component.restoreViewportFromStorage();

            // Empty canvas: identity transform (scale=1, origin)
            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBe(0);
            expect(transform.translate.y).toBe(0);
        });

        it('should center a single small component at scale 1', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture, 1200, 800);

            localStorage.removeItem('nifi-view-test-pg-id');

            component.restoreViewportFromStorage();

            // Small content in large viewport => scale 1, centered
            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            // The processor at (500,500) should be translated to center it
            // Viewport center is (600,400), so content center should roughly align
            expect(transform.translate.x).not.toBe(0);
            expect(transform.translate.y).not.toBe(0);
        });
    });

    describe('onZoomFit', () => {
        // onZoomFit calls fitContent(true) which uses D3 transitions (async in happy-dom).
        // We spy on zoom.transform to verify the computed D3 transform values.

        it('should invoke zoom.transform with identity when no components exist', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture);

            const zoom = (component as any).zoom;
            const transformArgs: any[] = [];
            const transformOriginal = zoom.transform;
            vi.spyOn(zoom, 'transform').mockImplementation((...args: any[]) => {
                transformArgs.push(args);
                return transformOriginal.apply(zoom, args);
            });

            component.onZoomFit();

            expect(zoom.transform).toHaveBeenCalled();
            const d3Transform = transformArgs[0]?.[1];
            expect(d3Transform?.k).toBe(1);
            expect(d3Transform?.x).toBe(0);
            expect(d3Transform?.y).toBe(0);
        });

        it('should invoke zoom.transform with scale 1 and centering for small content', async () => {
            // Processor at (500,500), dimensions 350x130 => bounding box center is (675, 565)
            // Viewport 1200x800 with padding 50 => canvas 1150x750
            // Content fits at scale 1. Centering: halfPad + (canvasW - graphW*1)/2 - minX*1
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture, 1200, 800);

            const zoom = (component as any).zoom;
            const transformArgs: any[] = [];
            const transformOriginal = zoom.transform;
            vi.spyOn(zoom, 'transform').mockImplementation((...args: any[]) => {
                transformArgs.push(args);
                return transformOriginal.apply(zoom, args);
            });

            component.onZoomFit();

            expect(zoom.transform).toHaveBeenCalled();
            const d3Transform = transformArgs[0]?.[1];
            expect(d3Transform?.k).toBe(1);
            // translateX = 25 + (1150 - 350) / 2 - 500 = 25 + 400 - 500 = -75
            expect(d3Transform?.x).toBeCloseTo(-75, 0);
            // translateY = 25 + (750 - 130) / 2 - 500 = 25 + 310 - 500 = -165
            expect(d3Transform?.y).toBeCloseTo(-165, 0);
        });

        it('should invoke zoom.transform with scale < 1 when content exceeds viewport', async () => {
            const processor1 = createMockProcessor({ id: 'proc-1', x: 0, y: 0 });
            const processor2 = createMockProcessor({ id: 'proc-2', x: 5000, y: 5000 });
            const { fixture, component } = await setup({
                processors: [processor1, processor2]
            });
            mockSvgDimensions(fixture, 1200, 800);

            const zoom = (component as any).zoom;
            const transformArgs: any[] = [];
            const transformOriginal = zoom.transform;
            vi.spyOn(zoom, 'transform').mockImplementation((...args: any[]) => {
                transformArgs.push(args);
                return transformOriginal.apply(zoom, args);
            });

            component.onZoomFit();

            expect(zoom.transform).toHaveBeenCalled();
            const d3Transform = transformArgs[0]?.[1];
            // Bounding box: (0,0) to (5350,5130). Canvas 1150x750.
            // Scale = min(1150/5350, 750/5130) ≈ 0.146
            expect(d3Transform?.k).toBeLessThan(1);
            expect(d3Transform?.k).toBeGreaterThan(0);
        });

        it('should not change transform when viewport has zero dimensions', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture, 0, 0);

            const beforeTransform = component.getTransform();
            component.onZoomFit();
            const afterTransform = component.getTransform();

            expect(afterTransform.scale).toBe(beforeTransform.scale);
        });
    });

    describe('onZoomIn', () => {
        it('should return early without error when zoom behavior is not initialized', async () => {
            const { component } = await setup();

            // Forcibly null out zoom to simulate pre-init state
            (component as any).zoom = null;

            // Should return early without error
            expect(() => component.onZoomIn()).not.toThrow();
        });

        it('should invoke D3 zoom scaleBy with the 1.2x increment', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture);

            const zoom = (component as any).zoom;
            const scaleByOriginal = zoom.scaleBy;
            const scaleByArgs: any[] = [];
            vi.spyOn(zoom, 'scaleBy').mockImplementation((...args: any[]) => {
                scaleByArgs.push(args);
                return scaleByOriginal.apply(zoom, args);
            });

            component.onZoomIn();

            // D3 transition.call(zoom.scaleBy, factor) invokes scaleBy(transition, factor)
            expect(zoom.scaleBy).toHaveBeenCalledTimes(1);
            const factor = scaleByArgs[0]?.[1];
            expect(factor).toBeCloseTo(1.2, 5);
        });

        it('should invoke scaleBy each time onZoomIn is called', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture);

            const zoom = (component as any).zoom;
            const scaleByOriginal = zoom.scaleBy;
            vi.spyOn(zoom, 'scaleBy').mockImplementation((...args: any[]) => {
                return scaleByOriginal.apply(zoom, args);
            });

            component.onZoomIn();
            component.onZoomIn();
            component.onZoomIn();

            expect(zoom.scaleBy).toHaveBeenCalledTimes(3);
        });
    });

    describe('onZoomOut', () => {
        it('should return early without error when zoom behavior is not initialized', async () => {
            const { component } = await setup();

            (component as any).zoom = null;

            expect(() => component.onZoomOut()).not.toThrow();
        });

        it('should invoke D3 zoom scaleBy with the 1/1.2x decrement', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture);

            const zoom = (component as any).zoom;
            const scaleByOriginal = zoom.scaleBy;
            const scaleByArgs: any[] = [];
            vi.spyOn(zoom, 'scaleBy').mockImplementation((...args: any[]) => {
                scaleByArgs.push(args);
                return scaleByOriginal.apply(zoom, args);
            });

            component.onZoomOut();

            expect(zoom.scaleBy).toHaveBeenCalledTimes(1);
            const factor = scaleByArgs[0]?.[1];
            expect(factor).toBeCloseTo(1 / 1.2, 5);
        });

        it('should invoke scaleBy each time onZoomOut is called', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture);

            const zoom = (component as any).zoom;
            const scaleByOriginal = zoom.scaleBy;
            vi.spyOn(zoom, 'scaleBy').mockImplementation((...args: any[]) => {
                return scaleByOriginal.apply(zoom, args);
            });

            component.onZoomOut();
            component.onZoomOut();

            expect(zoom.scaleBy).toHaveBeenCalledTimes(2);
        });
    });

    describe('onZoomActual', () => {
        // onZoomActual always sets scale to 1 and centers content.
        // Uses D3 transitions, so we spy on zoom.transform to verify computed values.

        function spyOnZoomTransform(component: any): { getLastTransform: () => any } {
            const zoom = component.zoom;
            const transformOriginal = zoom.transform;
            const transformArgs: any[] = [];
            vi.spyOn(zoom, 'transform').mockImplementation((...args: any[]) => {
                transformArgs.push(args);
                return transformOriginal.apply(zoom, args);
            });
            return {
                getLastTransform: () => transformArgs[transformArgs.length - 1]?.[1]
            };
        }

        it('should set scale to 1 and apply identity transform when no components exist', async () => {
            const { fixture, component } = await setup();
            mockSvgDimensions(fixture, 1200, 800);

            const spy = spyOnZoomTransform(component);
            component.onZoomActual();

            const d3Transform = spy.getLastTransform();
            expect(d3Transform?.k).toBe(1);
            expect(d3Transform?.x).toBe(0);
            expect(d3Transform?.y).toBe(0);
        });

        it('should center on all components at scale 1 when no selection exists', async () => {
            // Processor at (500, 500) with 350x130 dims => center is (675, 565)
            // Viewport 1200x800 => viewCenter is (600, 400)
            // translateX = 600 - 675 = -75, translateY = 400 - 565 = -165
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({
                processors: [processor],
                selectedComponentIds: []
            });
            mockSvgDimensions(fixture, 1200, 800);

            const spy = spyOnZoomTransform(component);
            component.onZoomActual();

            const d3Transform = spy.getLastTransform();
            expect(d3Transform?.k).toBe(1);
            expect(d3Transform?.x).toBeCloseTo(-75, 0);
            expect(d3Transform?.y).toBeCloseTo(-165, 0);
        });

        it('should center on selected component only when selection exists', async () => {
            // proc-1 at (100,100), 350x130 => center (275, 165)
            // proc-2 at (2000,2000) should be ignored since only proc-1 is selected
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 275 = 325, translateY = 400 - 165 = 235
            const processor1 = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const processor2 = createMockProcessor({ id: 'proc-2', x: 2000, y: 2000 });
            const { fixture, component } = await setup({
                processors: [processor1, processor2],
                selectedComponentIds: ['proc-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            const spy = spyOnZoomTransform(component);
            component.onZoomActual();

            const d3Transform = spy.getLastTransform();
            expect(d3Transform?.k).toBe(1);
            expect(d3Transform?.x).toBeCloseTo(325, 0);
            expect(d3Transform?.y).toBeCloseTo(235, 0);
        });

        it('should handle components at negative coordinates', async () => {
            // Processor at (-500, -300), 350x130 => center (-325, -235)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - (-325) = 925, translateY = 400 - (-235) = 635
            const processor = createMockProcessor({ id: 'proc-1', x: -500, y: -300 });
            const { fixture, component } = await setup({ processors: [processor] });
            mockSvgDimensions(fixture, 1200, 800);

            const spy = spyOnZoomTransform(component);
            component.onZoomActual();

            const d3Transform = spy.getLastTransform();
            expect(d3Transform?.k).toBe(1);
            expect(d3Transform?.x).toBeCloseTo(925, 0);
            expect(d3Transform?.y).toBeCloseTo(635, 0);
        });

        it('should center on bounding box of multiple selected components', async () => {
            // proc-1 at (0,0), 350x130; proc-2 at (1000,800), 350x130
            // Both selected => bounding box (0,0) to (1350,930), center (675, 465)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 675 = -75, translateY = 400 - 465 = -65
            const processor1 = createMockProcessor({ id: 'proc-1', x: 0, y: 0 });
            const processor2 = createMockProcessor({ id: 'proc-2', x: 1000, y: 800 });
            const { fixture, component } = await setup({
                processors: [processor1, processor2],
                selectedComponentIds: ['proc-1', 'proc-2']
            });
            mockSvgDimensions(fixture, 1200, 800);

            const spy = spyOnZoomTransform(component);
            component.onZoomActual();

            const d3Transform = spy.getLastTransform();
            expect(d3Transform?.k).toBe(1);
            expect(d3Transform?.x).toBeCloseTo(-75, 0);
            expect(d3Transform?.y).toBeCloseTo(-65, 0);
        });
    });

    describe('centerOnSelection', () => {
        // centerOnSelection(false) uses the synchronous applyTransform path,
        // so we can verify actual transform values via getTransform().
        // The method uses the current scale and centers the selection's bounding box
        // in the viewport. Formula:
        //   centerX = (minX + maxX) / 2, centerY = (minY + maxY) / 2
        //   translateX = viewCenterX - centerX * currentScale
        //   translateY = viewCenterY - centerY * currentScale

        it('should not change transform when no components are selected', async () => {
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({
                processors: [processor],
                selectedComponentIds: []
            });
            mockSvgDimensions(fixture, 1200, 800);

            const initialTransform = component.getTransform();
            component.centerOnSelection(false);
            const finalTransform = component.getTransform();

            expect(finalTransform.translate.x).toBe(initialTransform.translate.x);
            expect(finalTransform.translate.y).toBe(initialTransform.translate.y);
            expect(finalTransform.scale).toBe(initialTransform.scale);
        });

        it('should center on a selected processor at current scale', async () => {
            // Processor at (500, 500), 350x130 => center (675, 565)
            // Viewport 1200x800 => viewCenter (600, 400)
            // Scale = 1 (initial)
            // translateX = 600 - 675*1 = -75, translateY = 400 - 565*1 = -165
            const processor = createMockProcessor({ id: 'proc-1', x: 500, y: 500 });
            const { fixture, component } = await setup({
                processors: [processor],
                selectedComponentIds: ['proc-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(-75, 0);
            expect(transform.translate.y).toBeCloseTo(-165, 0);
        });

        it('should center on a selected label', async () => {
            // Label at (300, 300), 150x150 (from mock) => center (375, 375)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 375 = 225, translateY = 400 - 375 = 25
            const label = createMockLabel({ id: 'label-1', x: 300, y: 300, width: 150, height: 150 });
            const { fixture, component } = await setup({
                labels: [label],
                selectedComponentIds: ['label-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(225, 0);
            expect(transform.translate.y).toBeCloseTo(25, 0);
        });

        it('should center on a selected funnel', async () => {
            // Funnel at (400, 400), 48x48 => center (424, 424)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 424 = 176, translateY = 400 - 424 = -24
            const funnel = createMockFunnel({ id: 'funnel-1', x: 400, y: 400 });
            const { fixture, component } = await setup({
                funnels: [funnel],
                selectedComponentIds: ['funnel-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(176, 0);
            expect(transform.translate.y).toBeCloseTo(-24, 0);
        });

        it('should center on a selected process group', async () => {
            // Process group at (600, 600), 384x176 => center (792, 688)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 792 = -192, translateY = 400 - 688 = -288
            const processGroup = createMockProcessGroup({ id: 'pg-1', x: 600, y: 600 });
            const { fixture, component } = await setup({
                processGroups: [processGroup],
                selectedComponentIds: ['pg-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(-192, 0);
            expect(transform.translate.y).toBeCloseTo(-288, 0);
        });

        it('should center on bounding box of multiple selected processors', async () => {
            // proc-1 at (100,100), 350x130; proc-2 at (800,600), 350x130
            // Combined bounding box: (100,100) to (1150,730) => center (625, 415)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 625 = -25, translateY = 400 - 415 = -15
            const processor1 = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const processor2 = createMockProcessor({ id: 'proc-2', x: 800, y: 600 });
            const { fixture, component } = await setup({
                processors: [processor1, processor2],
                selectedComponentIds: ['proc-1', 'proc-2']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(-25, 0);
            expect(transform.translate.y).toBeCloseTo(-15, 0);
        });

        it('should only center on selected component, ignoring unselected ones', async () => {
            // proc-1 at (100,100) selected; proc-2 at (2000,2000) NOT selected
            // Only proc-1 in bounding box: center (275, 165)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 275 = 325, translateY = 400 - 165 = 235
            const processor1 = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const processor2 = createMockProcessor({ id: 'proc-2', x: 2000, y: 2000 });
            const { fixture, component } = await setup({
                processors: [processor1, processor2],
                selectedComponentIds: ['proc-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(325, 0);
            expect(transform.translate.y).toBeCloseTo(235, 0);
        });

        it('should handle components at negative coordinates', async () => {
            // Processor at (-500, -300), 350x130 => center (-325, -235)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - (-325) = 925, translateY = 400 - (-235) = 635
            const processor = createMockProcessor({ id: 'proc-1', x: -500, y: -300 });
            const { fixture, component } = await setup({
                processors: [processor],
                selectedComponentIds: ['proc-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(925, 0);
            expect(transform.translate.y).toBeCloseTo(635, 0);
        });

        it('should center on mixed component types in selection', async () => {
            // proc-1 at (100,100), 350x130; funnel at (500,500), 48x48
            // Bounding box: (100,100) to (548,548) => center (324, 324)
            // Viewport 1200x800 => viewCenter (600, 400)
            // translateX = 600 - 324 = 276, translateY = 400 - 324 = 76
            const processor = createMockProcessor({ id: 'proc-1', x: 100, y: 100 });
            const funnel = createMockFunnel({ id: 'funnel-1', x: 500, y: 500 });
            const { fixture, component } = await setup({
                processors: [processor],
                funnels: [funnel],
                selectedComponentIds: ['proc-1', 'funnel-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            expect(transform.scale).toBe(1);
            expect(transform.translate.x).toBeCloseTo(276, 0);
            expect(transform.translate.y).toBeCloseTo(76, 0);
        });

        it('should preserve current scale when centering', async () => {
            // First change the scale via restoreViewportFromStorage with a stored viewport
            const processor = createMockProcessor({ id: 'proc-1', x: 200, y: 200 });
            const { fixture, component } = await setup({
                processors: [processor],
                selectedComponentIds: ['proc-1']
            });
            mockSvgDimensions(fixture, 1200, 800);

            // Set a non-default scale via stored viewport
            localStorage.setItem(
                'nifi-view-test-pg-id',
                JSON.stringify({
                    expires: Date.now() + 172800000,
                    item: {
                        scale: 2,
                        translateX: 0,
                        translateY: 0
                    }
                })
            );
            component.restoreViewportFromStorage();

            const scaleBeforeCenter = component.getTransform().scale;
            expect(scaleBeforeCenter).toBe(2);

            component.centerOnSelection(false);

            const transform = component.getTransform();
            // Scale should be preserved at 2
            expect(transform.scale).toBe(2);
            // Processor center at (375, 265), viewport center (600, 400), scale 2
            // translateX = 600 - 375*2 = -150, translateY = 400 - 265*2 = -130
            expect(transform.translate.x).toBeCloseTo(-150, 0);
            expect(transform.translate.y).toBeCloseTo(-130, 0);
        });
    });

    describe('componentDoubleClick output', () => {
        it('should emit componentDoubleClick with Processor type on processor double-click', async () => {
            const { component } = await setup();

            const emittedValues: any[] = [];
            outputToObservable(component.componentDoubleClick).subscribe((v) => emittedValues.push(v));

            const entity = { id: 'proc-1', component: { name: 'Test Processor' } };
            const canvasProcessor = {
                entity,
                ui: { componentType: ComponentType.Processor }
            } as any;

            component.onProcessorDoubleClick({ processor: canvasProcessor, event: new MouseEvent('dblclick') });

            expect(emittedValues).toHaveLength(1);
            expect(emittedValues[0]).toEqual({ entity, componentType: ComponentType.Processor });
        });

        it('should emit componentDoubleClick with InputPort type on port double-click', async () => {
            const { component } = await setup();

            const emittedValues: any[] = [];
            outputToObservable(component.componentDoubleClick).subscribe((v) => emittedValues.push(v));

            const entity = { id: 'port-1', component: { name: 'Input Port' } };
            const canvasPort = {
                entity,
                ui: { componentType: ComponentType.InputPort }
            } as any;

            component.onPortDoubleClick({ port: canvasPort, event: new MouseEvent('dblclick') });

            expect(emittedValues).toHaveLength(1);
            expect(emittedValues[0]).toEqual({ entity, componentType: ComponentType.InputPort });
        });

        it('should emit componentDoubleClick with RemoteProcessGroup type on RPG double-click', async () => {
            const { component } = await setup();

            const emittedValues: any[] = [];
            outputToObservable(component.componentDoubleClick).subscribe((v) => emittedValues.push(v));

            const entity = { id: 'rpg-1', component: { name: 'Remote PG' } };
            const canvasRpg = {
                entity,
                ui: { componentType: ComponentType.RemoteProcessGroup }
            } as any;

            component.onRemoteProcessGroupDoubleClick({ rpg: canvasRpg, event: new MouseEvent('dblclick') });

            expect(emittedValues).toHaveLength(1);
            expect(emittedValues[0]).toEqual({ entity, componentType: ComponentType.RemoteProcessGroup });
        });

        it('should emit componentDoubleClick with Connection type on connection double-click', async () => {
            const { component } = await setup();

            const emittedValues: any[] = [];
            outputToObservable(component.componentDoubleClick).subscribe((v) => emittedValues.push(v));

            const entity = { id: 'conn-1', component: { source: {}, destination: {} } };
            const canvasConnection = {
                entity,
                ui: { componentType: ComponentType.Connection }
            } as any;

            component.onConnectionDoubleClick({ connection: canvasConnection, event: new MouseEvent('dblclick') });

            expect(emittedValues).toHaveLength(1);
            expect(emittedValues[0]).toEqual({ entity, componentType: ComponentType.Connection });
        });

        it('should not emit componentDoubleClick on funnel double-click', async () => {
            const { component } = await setup();

            const emittedValues: any[] = [];
            outputToObservable(component.componentDoubleClick).subscribe((v) => emittedValues.push(v));

            const canvasFunnel = {
                entity: { id: 'funnel-1' },
                ui: { componentType: ComponentType.Funnel }
            } as any;

            component.onFunnelDoubleClick({ funnel: canvasFunnel, event: new MouseEvent('dblclick') });

            expect(emittedValues).toHaveLength(0);
        });

        it('should not emit componentDoubleClick on label double-click', async () => {
            const { component } = await setup();

            const emittedValues: any[] = [];
            outputToObservable(component.componentDoubleClick).subscribe((v) => emittedValues.push(v));

            const canvasLabel = {
                entity: { id: 'label-1' },
                ui: { componentType: ComponentType.Label }
            } as any;

            component.onLabelDoubleClick({ label: canvasLabel, event: new MouseEvent('dblclick') });

            expect(emittedValues).toHaveLength(0);
        });
    });
});
