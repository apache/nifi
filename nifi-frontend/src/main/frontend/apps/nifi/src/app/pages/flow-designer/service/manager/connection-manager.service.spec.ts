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

import { ConnectionManager } from './connection-manager.service';
import { CanvasState } from '../../state';
import { flowFeatureKey } from '../../state/flow';
import * as fromFlow from '../../state/flow/flow.reducer';
import { transformFeatureKey } from '../../state/transform';
import * as fromTransform from '../../state/transform/transform.reducer';
import { provideMockStore } from '@ngrx/store/testing';
import { selectFlowState } from '../../state/flow/flow.selectors';
import { selectTransform } from '../../state/transform/transform.selectors';
import { controllerServicesFeatureKey } from '../../state/controller-services';
import * as fromControllerServices from '../../state/controller-services/controller-services.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import * as fromUser from '../../../../state/current-user/current-user.reducer';
import { parameterFeatureKey } from '../../state/parameter';
import * as fromParameter from '../../state/parameter/parameter.reducer';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';
import { queueFeatureKey } from '../../../queue/state';
import * as fromQueue from '../../state/queue/queue.reducer';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { flowAnalysisFeatureKey } from '../../state/flow-analysis';
import * as fromFlowAnalysis from '../../state/flow-analysis/flow-analysis.reducer';

describe('ConnectionManager', () => {
    let service: ConnectionManager;

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
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                }
            ]
        });
        service = TestBed.inject(ConnectionManager);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    describe('isRetryConfigured', () => {
        it('should return true when retriedRelationships exist and have length > 0', () => {
            const connection = {
                retriedRelationships: ['failure', 'retry']
            };

            // Access private method via bracket notation for testing
            const result = (service as any).isRetryConfigured(connection);

            expect(result).toBe(true);
        });

        it('should return false when retriedRelationships is null', () => {
            const connection = {
                retriedRelationships: null
            };

            const result = (service as any).isRetryConfigured(connection);

            expect(result).toBe(false);
        });

        it('should return false when retriedRelationships is undefined', () => {
            const connection = {
                retriedRelationships: undefined
            };

            const result = (service as any).isRetryConfigured(connection);

            expect(result).toBe(false);
        });

        it('should return false when retriedRelationships is empty array', () => {
            const connection = {
                retriedRelationships: []
            };

            const result = (service as any).isRetryConfigured(connection);

            expect(result).toBe(false);
        });

        it('should return true when retriedRelationships has one relationship', () => {
            const connection = {
                retriedRelationships: ['failure']
            };

            const result = (service as any).isRetryConfigured(connection);

            expect(result).toBe(true);
        });
    });
});
