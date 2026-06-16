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
import * as d3 from 'd3';

import { CanvasView } from './canvas-view.service';
import { CanvasState } from '../state';
import { flowFeatureKey } from '../state/flow';
import * as fromFlow from '../state/flow/flow.reducer';
import { transformFeatureKey } from '../state/transform';
import * as fromTransform from '../state/transform/transform.reducer';
import { provideMockStore } from '@ngrx/store/testing';
import { selectFlowState } from '../state/flow/flow.selectors';
import { selectTransform } from '../state/transform/transform.selectors';
import { controllerServicesFeatureKey } from '../state/controller-services';
import * as fromControllerServices from '../state/controller-services/controller-services.reducer';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import * as fromUser from '../../../state/current-user/current-user.reducer';
import { parameterFeatureKey } from '../state/parameter';
import * as fromParameter from '../state/parameter/parameter.reducer';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../state/flow-configuration/flow-configuration.reducer';
import { flowAnalysisFeatureKey } from '../state/flow-analysis';
import * as fromFlowAnalysis from '../state/flow-analysis/flow-analysis.reducer';
import { MAX_ABS_TRANSLATE } from '@nifi/shared';

/**
 * Attach a real DOM structure that CanvasView.init() and its guard paths
 * depend on. Returns cleanup/spy handles.
 */
function mountCanvasDom(service: CanvasView) {
    const container = document.createElement('div');
    container.id = 'canvas-container';
    document.body.appendChild(container);

    const svgEl = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svgEl.id = 'canvas';
    container.appendChild(svgEl);

    const svg = d3.select(svgEl);
    const canvasGroupEl = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    svgEl.appendChild(canvasGroupEl);
    const canvas = d3.select(canvasGroupEl);

    // Mock behavior.transform so tests can spy on what value is applied
    const transformSpy = vi.fn();
    service['behavior'] = {
        transform: transformSpy,
        translateBy: vi.fn(),
        scaleBy: vi.fn(),
        scaleExtent: () => service['behavior'],
        on: () => service['behavior']
    };
    service['svg'] = svg;
    service['canvas'] = canvas;

    return {
        container,
        svgEl,
        transformSpy,
        cleanup() {
            document.body.removeChild(container);
        }
    };
}

describe('CanvasView', () => {
    let service: CanvasView;

    beforeEach(() => {
        const initialState: CanvasState = {
            [flowFeatureKey]: fromFlow.initialState,
            [transformFeatureKey]: fromTransform.initialState,
            [controllerServicesFeatureKey]: fromControllerServices.initialState,
            [parameterFeatureKey]: fromParameter.initialState,
            [flowAnalysisFeatureKey]: fromFlowAnalysis.initialState
        };

        TestBed.configureTestingModule({
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectFlowState,
                            value: initialState[flowFeatureKey]
                        },
                        {
                            selector: selectTransform,
                            value: initialState[transformFeatureKey]
                        },
                        {
                            selector: selectCurrentUser,
                            value: fromUser.initialState.user
                        },
                        {
                            selector: selectFlowConfiguration,
                            value: fromFlowConfiguration.initialState.flowConfiguration
                        }
                    ]
                })
            ]
        });
        service = TestBed.inject(CanvasView);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    describe('CanvasView transform safety', () => {
        describe('transform() — early-return guard', () => {
            it('calls behavior.transform for a valid translate and scale', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([100, 200], 1);
                expect(transformSpy).toHaveBeenCalledTimes(1);
                cleanup();
            });

            it('rejects a catastrophic-finite translateX (~9e307)', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([9e307, 0], 1);
                expect(transformSpy).not.toHaveBeenCalled();
                cleanup();
            });

            it('rejects a catastrophic-finite translateY', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([0, MAX_ABS_TRANSLATE + 1], 1);
                expect(transformSpy).not.toHaveBeenCalled();
                cleanup();
            });

            it('rejects Infinity in translate', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([Number.POSITIVE_INFINITY, 0], 1);
                expect(transformSpy).not.toHaveBeenCalled();
                cleanup();
            });

            it('rejects NaN translate', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([NaN, 0], 1);
                expect(transformSpy).not.toHaveBeenCalled();
                cleanup();
            });

            it('rejects a scale above MAX_SCALE', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([0, 0], 100);
                expect(transformSpy).not.toHaveBeenCalled();
                cleanup();
            });

            it('rejects a near-zero scale (division-by-zero hazard)', () => {
                const { cleanup, transformSpy } = mountCanvasDom(service);
                service.transform([0, 0], 0.0001);
                expect(transformSpy).not.toHaveBeenCalled();
                cleanup();
            });
        });

        describe('getSelectionBoundingClientRect()', () => {
            it('returns null for an empty d3 selection', () => {
                const { cleanup } = mountCanvasDom(service);
                const emptySelection = d3.selectAll('.nonexistent-element-12345');
                const result = service.getSelectionBoundingClientRect(emptySelection);
                expect(result).toBeNull();
                cleanup();
            });

            it('returns a bbox object for a non-empty selection', () => {
                const { container, cleanup } = mountCanvasDom(service);
                const node = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
                container.appendChild(node);
                // Stub getBoundingClientRect for the test environment
                node.getBoundingClientRect = () => ({ x: 10, y: 20, right: 110, bottom: 120 }) as DOMRect;
                const sel = d3.select(node);
                service['x'] = 0;
                service['y'] = 0;
                service['k'] = 1;
                const result = service.getSelectionBoundingClientRect(sel);
                expect(result).not.toBeNull();
                expect(result?.width).toBeGreaterThan(0);
                cleanup();
            });
        });

        describe('getCanvasPosition()', () => {
            it('returns a position when transform is valid', () => {
                const { container, cleanup } = mountCanvasDom(service);
                container.getBoundingClientRect = () => ({ left: 0, top: 0, width: 800, height: 600 }) as DOMRect;
                service['k'] = 1;
                service['x'] = 0;
                service['y'] = 0;
                const result = service.getCanvasPosition({ x: 400, y: 300 });
                expect(result).not.toBeNull();
                expect(Number.isFinite(result!.x)).toBe(true);
                cleanup();
            });

            it('returns null when k is near-zero (overflow result exceeds MAX_ABS_COORD)', () => {
                const { container, cleanup } = mountCanvasDom(service);
                container.getBoundingClientRect = () => ({ left: 0, top: 0, width: 800, height: 600 }) as DOMRect;
                // k = 1e-15 produces x = 400 / 1e-15 = 4e17, way above MAX_ABS_COORD
                service['k'] = 1e-15;
                service['x'] = 0;
                service['y'] = 0;
                const result = service.getCanvasPosition({ x: 400, y: 300 });
                expect(result).toBeNull();
                cleanup();
            });

            it('returns null when k is NaN', () => {
                const { container, cleanup } = mountCanvasDom(service);
                container.getBoundingClientRect = () => ({ left: 0, top: 0, width: 800, height: 600 }) as DOMRect;
                service['k'] = NaN;
                service['x'] = 0;
                service['y'] = 0;
                const result = service.getCanvasPosition({ x: 400, y: 300 });
                expect(result).toBeNull();
                cleanup();
            });
        });

        describe('fit() — zero-dimension container guard', () => {
            it('returns early without throwing when container has zero width', () => {
                const { container, cleanup } = mountCanvasDom(service);
                container.getBoundingClientRect = () => ({ width: 0, height: 600 }) as DOMRect;
                service['k'] = 1;
                expect(() => service.fit(false)).not.toThrow();
                cleanup();
            });

            it('returns early without throwing when container has zero height', () => {
                const { container, cleanup } = mountCanvasDom(service);
                container.getBoundingClientRect = () => ({ width: 800, height: 0 }) as DOMRect;
                service['k'] = 1;
                expect(() => service.fit(false)).not.toThrow();
                cleanup();
            });
        });
    });
});
