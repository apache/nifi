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

/*
  Canvas Positioning/Transforms
 */

import { transformFeatureKey, CanvasTransform } from './transform';
import { flowFeatureKey, FlowState } from './flow';
import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { transformReducer } from './transform/transform.reducer';
import { flowReducer } from './flow/flow.reducer';
import { controllerServicesFeatureKey, ControllerServicesState } from './controller-services';
import { controllerServicesReducer } from './controller-services/controller-services.reducer';
import { parameterFeatureKey, ParameterState } from './parameter';
import { parameterReducer } from './parameter/parameter.reducer';
import { queueFeatureKey } from '../../queue/state';
import { QueueState } from './queue';
import { queueReducer } from './queue/queue.reducer';
import { FlowAnalysisState, flowAnalysisFeatureKey } from './flow-analysis';
import { flowAnalysisReducer } from './flow-analysis/flow-analysis.reducer';

export const canvasFeatureKey = 'canvas';

export interface CanvasState {
    [flowFeatureKey]: FlowState;
    [transformFeatureKey]: CanvasTransform;
    [controllerServicesFeatureKey]: ControllerServicesState;
    [parameterFeatureKey]: ParameterState;
    [queueFeatureKey]: QueueState;
    [flowAnalysisFeatureKey]: FlowAnalysisState;
}

export function reducers(state: CanvasState | undefined, action: Action) {
    return combineReducers({
        [flowFeatureKey]: flowReducer,
        [transformFeatureKey]: transformReducer,
        [controllerServicesFeatureKey]: controllerServicesReducer,
        [parameterFeatureKey]: parameterReducer,
        [queueFeatureKey]: queueReducer,
        [flowAnalysisFeatureKey]: flowAnalysisReducer
    })(state, action);
}

export const selectCanvasState = createFeatureSelector<CanvasState>(canvasFeatureKey);
