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

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { GeneralState, generalFeatureKey } from './general';
import { generalReducer } from './general/general.reducer';
import {
    managementControllerServicesFeatureKey,
    ManagementControllerServicesState
} from './management-controller-services';
import { managementControllerServicesReducer } from './management-controller-services/management-controller-services.reducer';

export const settingsFeatureKey = 'settings';

export interface SettingsState {
    [generalFeatureKey]: GeneralState;
    [managementControllerServicesFeatureKey]: ManagementControllerServicesState;
}

export function reducers(state: SettingsState | undefined, action: Action) {
    return combineReducers({
        [generalFeatureKey]: generalReducer,
        [managementControllerServicesFeatureKey]: managementControllerServicesReducer
    })(state, action);
}

export const selectSettingsState = createFeatureSelector<SettingsState>(settingsFeatureKey);
