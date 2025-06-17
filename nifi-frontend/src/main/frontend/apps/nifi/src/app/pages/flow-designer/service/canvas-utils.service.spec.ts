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

import { CanvasUtils } from './canvas-utils.service';
import { CanvasState } from '../state';
import { flowFeatureKey } from '../state/flow';
import * as fromFlow from '../state/flow/flow.reducer';
import { transformFeatureKey } from '../state/transform';
import * as fromTransform from '../state/transform/transform.reducer';
import { provideMockStore } from '@ngrx/store/testing';
import { selectFlowState } from '../state/flow/flow.selectors';
import { controllerServicesFeatureKey } from '../state/controller-services';
import * as fromControllerServices from '../state/controller-services/controller-services.reducer';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import * as fromUser from '../../../state/current-user/current-user.reducer';
import { parameterFeatureKey } from '../state/parameter';
import * as fromParameter from '../state/parameter/parameter.reducer';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../state/flow-configuration/flow-configuration.reducer';
import { queueFeatureKey } from '../../queue/state';
import * as fromQueue from '../state/queue/queue.reducer';
import { flowAnalysisFeatureKey } from '../state/flow-analysis';
import * as fromFlowAnalysis from '../state/flow-analysis/flow-analysis.reducer';
import { ComponentType } from '@nifi/shared';
import * as d3 from 'd3';

describe('CanvasUtils', () => {
    let service: CanvasUtils;

    beforeEach(() => {
        const initialState: CanvasState = {
            [flowFeatureKey]: fromFlow.initialState,
            [transformFeatureKey]: fromTransform.initialState,
            [controllerServicesFeatureKey]: fromControllerServices.initialState,
            [parameterFeatureKey]: fromParameter.initialState,
            [queueFeatureKey]: fromQueue.initialState,
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
        service = TestBed.inject(CanvasUtils);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    describe('supportsStopFlowVersioning', () => {
        it('should return null if selection is non empty and version control information is missing', () => {
            const pgDatum = {
                id: '1',
                type: ComponentType.ProcessGroup,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '1',
                    name: 'Test Process Group',
                    versionControlInformation: null
                }
            };
            const selection = d3.select(document.createElement('div')).classed('process-group', true).datum(pgDatum);
            expect(service.getFlowVersionControlInformation(selection)).toBe(null);
        });

        it('should return vci if selection is non empty and version control information is present', () => {
            const versionControlInformation = {
                groupId: '1',
                registryId: '324e0ab1-0197-1000-ffff-ffffb3123c5c',
                registryName: 'ConnectorFlowRegistryClient',
                branch: 'main',
                bucketId: 'connectors',
                bucketName: 'connectors',
                flowId: 'kafka-json-sasl-topic2table-schemaev',
                flowName: 'kafka-json-sasl-topic2table-schemaev',
                version: '0.1.0-f47ff72',
                state: 'UP_TO_DATE',
                stateExplanation: 'Flow version is current'
            };
            const pgDatum = {
                id: '1',
                type: ComponentType.ProcessGroup,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '1',
                    name: 'Test Process Group',
                    versionControlInformation
                }
            };
            const selection = d3.select(document.createElement('div')).classed('process-group', true).datum(pgDatum);
            expect(service.getFlowVersionControlInformation(selection)).toBe(versionControlInformation);
        });
    });
});
