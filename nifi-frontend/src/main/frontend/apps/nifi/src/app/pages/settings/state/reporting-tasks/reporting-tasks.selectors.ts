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
import { ReportingTaskEntity, reportingTasksFeatureKey, ReportingTasksState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectReportingTasksState = createSelector(
    selectSettingsState,
    (state: SettingsState) => state[reportingTasksFeatureKey]
);

export const selectSaving = createSelector(selectReportingTasksState, (state: ReportingTasksState) => state.saving);

export const selectStatus = createSelector(selectReportingTasksState, (state: ReportingTasksState) => state.status);

export const selectReportingTaskIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the reporting task from the route
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedReportingTask = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectReportingTasks = createSelector(
    selectReportingTasksState,
    (state: ReportingTasksState) => state.reportingTasks
);

export const selectTask = (id: string) =>
    createSelector(selectReportingTasks, (tasks: ReportingTaskEntity[]) => tasks.find((task) => id == task.id));
