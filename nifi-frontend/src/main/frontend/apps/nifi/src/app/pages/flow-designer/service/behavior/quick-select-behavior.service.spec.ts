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

import { QuickSelectBehavior } from './quick-select-behavior.service';
import { provideMockStore } from '@ngrx/store/testing';
import { selectFlowState } from '../../state/flow/flow.selectors';
import { CanvasState } from '../../state';
import { flowFeatureKey } from '../../state/flow';
import * as fromFlow from '../../state/flow/flow.reducer';
import { transformFeatureKey } from '../../state/transform';
import * as fromTransform from '../../state/transform/transform.reducer';
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
import { flowAnalysisFeatureKey } from '../../state/flow-analysis';
import * as fromFlowAnalysis from '../../state/flow-analysis/flow-analysis.reducer';

describe('QuickSelectBehavior', () => {
    let service: QuickSelectBehavior;

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
        service = TestBed.inject(QuickSelectBehavior);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });
});
