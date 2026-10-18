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
import { TestBed } from '@angular/core/testing';

import { BirdseyeView } from './birdseye-view.service';
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

describe('BirdseyeView', () => {
    let service: BirdseyeView;

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
        service = TestBed.inject(BirdseyeView);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    it('keeps the minimap scale independent of the header and sidebar screen offsets', () => {
        const host = document.createElement('div');
        host.innerHTML =
            '<div id="canvas-container"><svg id="canvas"></svg></div><div id="birdseye"><canvas id="birdseye-canvas"></canvas><svg><g class="test-birdseye"><g class="test-components"></g></g><rect class="birdseye-brush"></rect></svg></div>';
        document.body.appendChild(host);
        const container = host.querySelector('#canvas-container')!;
        const canvas = host.querySelector('#canvas')!;
        const birdseye = host.querySelector('#birdseye')!;
        const context = {
            save: vi.fn(),
            restore: vi.fn(),
            setTransform: vi.fn(),
            clearRect: vi.fn(),
            translate: vi.fn(),
            scale: vi.fn()
        };
        vi.spyOn(host.querySelector('canvas')!, 'getContext').mockReturnValue(context as any);
        for (const manager of [
            'processorManager',
            'processGroupManager',
            'remoteProcessGroupManager',
            'portManager',
            'funnelManager',
            'labelManager'
        ] as const) {
            vi.spyOn(service[manager], 'selectAll').mockReturnValue(d3.selectAll([]) as any);
        }
        service['birdseyeGroup'] = d3.select(host.querySelector('.test-birdseye'));
        service['componentGroup'] = d3.select(host.querySelector('.test-components'));
        d3.select(host.querySelector('.birdseye-brush')).datum({ x: 0, y: 0 });
        service['initialized'] = true;
        service['navigationCollapsed'] = false;
        service['k'] = 1;
        service['x'] = 0;
        service['y'] = 0;
        birdseye.getBoundingClientRect = () => ({ width: 200, height: 150 }) as DOMRect;
        try {
            for (const [left, top] of [
                [0, 0],
                [182, 132]
            ]) {
                container.getBoundingClientRect = () => ({ left, top, width: 800, height: 600 }) as DOMRect;
                canvas.getBoundingClientRect = () =>
                    ({ left: left + 100, top: top + 100, right: left + 1200, bottom: top + 800 }) as DOMRect;
                service.refresh();
                expect(service['birdseyeGroup'].attr('transform')).toBe('scale(' + 1 / 6 + ')');
                expect(service['componentGroup'].attr('transform')).toBe('translate(0,0)');
            }
        } finally {
            host.remove();
        }
    });

    describe('refresh() — magnitude guard', () => {
        it('returns early without throwing when translateX is catastrophic-finite', () => {
            service['x'] = 7.490388061926315e307;
            service['y'] = 0;
            service['k'] = 1;
            service['initialized'] = true;
            service['navigationCollapsed'] = false;
            expect(() => service.refresh()).not.toThrow();
        });

        it('returns early without throwing when k is near-zero', () => {
            service['x'] = 0;
            service['y'] = 0;
            service['k'] = 0.0001;
            service['initialized'] = true;
            service['navigationCollapsed'] = false;
            expect(() => service.refresh()).not.toThrow();
        });

        it('returns early when k is Infinity', () => {
            service['x'] = 0;
            service['y'] = 0;
            service['k'] = Infinity;
            service['initialized'] = true;
            service['navigationCollapsed'] = false;
            expect(() => service.refresh()).not.toThrow();
        });
    });
});
