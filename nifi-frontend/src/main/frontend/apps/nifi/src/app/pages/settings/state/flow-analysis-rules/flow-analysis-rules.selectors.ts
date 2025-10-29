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

import { createSelector } from '@ngrx/store';
import { selectSettingsState, SettingsState } from '../index';
import { FlowAnalysisRuleEntity, flowAnalysisRulesFeatureKey, FlowAnalysisRulesState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectFlowAnalysisRulesState = createSelector(
    selectSettingsState,
    (state: SettingsState) => state[flowAnalysisRulesFeatureKey]
);

export const selectSaving = createSelector(
    selectFlowAnalysisRulesState,
    (state: FlowAnalysisRulesState) => state.saving
);

export const selectLoadedTimestamp = createSelector(
    selectFlowAnalysisRulesState,
    (state: FlowAnalysisRulesState) => state.loadedTimestamp
);

export const selectFlowAnalysisRuleIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the rule from the route
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedFlowAnalysisRule = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectFlowAnalysisRules = createSelector(
    selectFlowAnalysisRulesState,
    (state: FlowAnalysisRulesState) => state.flowAnalysisRules
);

export const selectRule = (id: string) =>
    createSelector(selectFlowAnalysisRules, (tasks: FlowAnalysisRuleEntity[]) => tasks.find((task) => id == task.id));
