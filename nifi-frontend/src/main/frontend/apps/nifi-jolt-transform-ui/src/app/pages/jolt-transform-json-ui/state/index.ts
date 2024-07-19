/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { JoltTransformJsonPropertyState } from './jolt-transform-json-property';
import { joltTransformJsonPropertyReducer } from './jolt-transform-json-property/jolt-transform-json-property.reducer';
import { JoltTransformJsonTransformState } from './jolt-transform-json-transform';
import { joltTransformJsonTransformReducer } from './jolt-transform-json-transform/jolt-transform-json-transform.reducer';
import { JoltTransformJsonProcessorDetailsState } from './jolt-transform-json-processor-details';
import { joltTransformJsonProcessorDetailsReducer } from './jolt-transform-json-processor-details/jolt-transform-json-processor-details.reducer';
import { joltTransformJsonValidateReducer } from './jolt-transform-json-validate/jolt-transform-json-validate.reducer';
import { JoltTransformJsonValidateState } from './jolt-transform-json-validate';

export const joltTransformJsonUiFeatureKey = 'joltTransformJsonUi';
export const joltTransformJsonProcessorDetailsFeatureKey = 'joltTransformJsonProcessorDetails';
export const joltTransformJsonPropertyFeatureKey = 'joltTransformJsonProperty';
export const joltTransformJsonTransformFeatureKey = 'joltTransformJsonTransform';
export const joltTransformJsonValidateFeatureKey = 'joltTransformJsonValidate';

export interface JoltTransformJsonUiState {
    [joltTransformJsonProcessorDetailsFeatureKey]: JoltTransformJsonProcessorDetailsState;
    [joltTransformJsonPropertyFeatureKey]: JoltTransformJsonPropertyState;
    [joltTransformJsonTransformFeatureKey]: JoltTransformJsonTransformState;
    [joltTransformJsonValidateFeatureKey]: JoltTransformJsonValidateState;
}

export function reducers(state: any, action: Action) {
    return combineReducers({
        [joltTransformJsonProcessorDetailsFeatureKey]: joltTransformJsonProcessorDetailsReducer,
        [joltTransformJsonPropertyFeatureKey]: joltTransformJsonPropertyReducer,
        [joltTransformJsonTransformFeatureKey]: joltTransformJsonTransformReducer,
        [joltTransformJsonValidateFeatureKey]: joltTransformJsonValidateReducer
    })(state, action);
}

export const selectJoltTransformJsonUiState =
    createFeatureSelector<JoltTransformJsonUiState>(joltTransformJsonUiFeatureKey);
