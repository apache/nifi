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

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { GeneralState, generalFeatureKey } from './general';
import { generalReducer } from './general/general.reducer';
import {
    managementControllerServicesFeatureKey,
    ManagementControllerServicesState
} from './management-controller-services';
import { managementControllerServicesReducer } from './management-controller-services/management-controller-services.reducer';
import { reportingTasksFeatureKey, ReportingTasksState } from './reporting-tasks';
import { reportingTasksReducer } from './reporting-tasks/reporting-tasks.reducer';
import { registryClientsFeatureKey, RegistryClientsState } from './registry-clients';
import { registryClientsReducer } from './registry-clients/registry-clients.reducer';
import { flowAnalysisRulesFeatureKey, FlowAnalysisRulesState } from './flow-analysis-rules';
import { flowAnalysisRulesReducer } from './flow-analysis-rules/flow-analysis-rules.reducer';
import { parameterProvidersFeatureKey, ParameterProvidersState } from './parameter-providers';
import { parameterProvidersReducer } from './parameter-providers/parameter-providers.reducer';

export const settingsFeatureKey = 'settings';

export interface SettingsState {
    [generalFeatureKey]: GeneralState;
    [managementControllerServicesFeatureKey]: ManagementControllerServicesState;
    [reportingTasksFeatureKey]: ReportingTasksState;
    [flowAnalysisRulesFeatureKey]: FlowAnalysisRulesState;
    [registryClientsFeatureKey]: RegistryClientsState;
    [parameterProvidersFeatureKey]: ParameterProvidersState;
}

export function reducers(state: SettingsState | undefined, action: Action) {
    return combineReducers({
        [generalFeatureKey]: generalReducer,
        [managementControllerServicesFeatureKey]: managementControllerServicesReducer,
        [reportingTasksFeatureKey]: reportingTasksReducer,
        [flowAnalysisRulesFeatureKey]: flowAnalysisRulesReducer,
        [registryClientsFeatureKey]: registryClientsReducer,
        [parameterProvidersFeatureKey]: parameterProvidersReducer
    })(state, action);
}

export const selectSettingsState = createFeatureSelector<SettingsState>(settingsFeatureKey);
