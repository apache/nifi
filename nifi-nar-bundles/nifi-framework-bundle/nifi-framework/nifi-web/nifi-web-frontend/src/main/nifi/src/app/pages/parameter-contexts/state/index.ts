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
  Parameter Contexts
 */

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { parameterContextListingFeatureKey, ParameterContextListingState } from './parameter-context-listing';
import { parameterContextListingReducer } from './parameter-context-listing/parameter-context-listing.reducer';

export const parameterContextsFeatureKey = 'parameterContexts';

export interface ParameterContextsState {
    [parameterContextListingFeatureKey]: ParameterContextListingState;
}

export function reducers(state: ParameterContextsState | undefined, action: Action) {
    return combineReducers({
        [parameterContextListingFeatureKey]: parameterContextListingReducer
    })(state, action);
}

export const selectParameterContextState = createFeatureSelector<ParameterContextsState>(parameterContextsFeatureKey);
