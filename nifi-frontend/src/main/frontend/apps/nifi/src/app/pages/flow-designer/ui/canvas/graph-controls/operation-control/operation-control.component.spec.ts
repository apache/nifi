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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import * as d3 from 'd3';
import { NiFiCommon } from '@nifi/shared';

import { OperationControl } from './operation-control.component';
import { DRAINING_DESCRIPTION, getDrainingTooltip } from '../../../../../../ui/common/drain/drain-summary.utils';
import { createProcessGroupDrainSummary } from '../../../../../../testing/process-group-drain-fixture';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../state/flow/flow.reducer';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { initialState as initialErrorState } from '../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../state/current-user';
import { flowConfigurationFeatureKey } from '../../../../../../state/flow-configuration';
import { initialState as flowConfigurationInitialState } from '../../../../../../state/flow-configuration/flow-configuration.reducer';
import { canvasFeatureKey } from '../../../../state';
import { flowFeatureKey } from '../../../../state/flow';
import { transformFeatureKey } from '../../../../state/transform';
import { initialState as transformInitialState } from '../../../../state/transform/transform.reducer';

describe('OperationControl', () => {
    let component: OperationControl;
    let fixture: ComponentFixture<OperationControl>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [OperationControl, HttpClientTestingModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [flowConfigurationFeatureKey]: flowConfigurationInitialState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState,
                            [transformFeatureKey]: transformInitialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(OperationControl);
        component = fixture.componentInstance;
        component.breadcrumbEntity = {
            id: '',
            permissions: {
                canRead: false,
                canWrite: false
            },
            versionedFlowState: '',
            breadcrumb: {
                id: '',
                name: ''
            }
        };
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('isDraining', () => {
        function processGroupSelection(datum: unknown) {
            return d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'g')).datum(datum);
        }

        it('reports the current process group drain when nothing is selected', () => {
            vi.spyOn(component.canvasUtils, 'getCurrentProcessGroupDrainSummary').mockReturnValue(
                createProcessGroupDrainSummary()
            );

            expect(component.isDraining(d3.selectAll<Element, unknown>([]))).toBe(true);
        });

        it('reports no drain when the current process group has no drain summary', () => {
            vi.spyOn(component.canvasUtils, 'getCurrentProcessGroupDrainSummary').mockReturnValue(null);

            expect(component.isDraining(d3.selectAll<Element, unknown>([]))).toBe(false);
        });

        it('reports a drain on a selected process group', () => {
            vi.spyOn(component.canvasUtils, 'isProcessGroup').mockReturnValue(true);

            expect(
                component.isDraining(
                    processGroupSelection({
                        drainSummary: {
                            id: 'drain-1',
                            prepared: true,
                            originalCount: 1,
                            originalSize: 1,
                            currentCount: 1,
                            currentSize: 1
                        }
                    })
                )
            ).toBe(true);
            expect(component.isDraining(processGroupSelection({}))).toBe(false);
        });

        it('ignores selections that are not a single process group', () => {
            vi.spyOn(component.canvasUtils, 'isProcessGroup').mockReturnValue(false);

            expect(
                component.isDraining(
                    processGroupSelection({
                        drainSummary: {
                            id: 'drain-1',
                            prepared: true,
                            originalCount: 1,
                            originalSize: 1,
                            currentCount: 1,
                            currentSize: 1
                        }
                    })
                )
            ).toBe(false);
        });
    });

    describe('getDrainingTooltip', () => {
        function processGroupSelection(datum: unknown) {
            return d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'g')).datum(datum);
        }

        it('uses the current process group drain summary when nothing is selected', () => {
            const drainSummary = createProcessGroupDrainSummary();
            vi.spyOn(component.canvasUtils, 'getCurrentProcessGroupDrainSummary').mockReturnValue(drainSummary);

            expect(component.getDrainingTooltip(d3.selectAll<Element, unknown>([]))).toBe(
                getDrainingTooltip(drainSummary, TestBed.inject(NiFiCommon))
            );
        });

        it('falls back to the generic draining sentence when the current group has no summary', () => {
            vi.spyOn(component.canvasUtils, 'getCurrentProcessGroupDrainSummary').mockReturnValue(null);

            expect(component.getDrainingTooltip(d3.selectAll<Element, unknown>([]))).toBe(DRAINING_DESCRIPTION);
        });

        it('uses the selected process group drain summary', () => {
            const drainSummary = createProcessGroupDrainSummary({ currentCount: 0, currentSize: 0 });
            vi.spyOn(component.canvasUtils, 'isProcessGroup').mockReturnValue(true);

            expect(component.getDrainingTooltip(processGroupSelection({ drainSummary }))).toBe(
                getDrainingTooltip(drainSummary, TestBed.inject(NiFiCommon))
            );
        });
    });
});
