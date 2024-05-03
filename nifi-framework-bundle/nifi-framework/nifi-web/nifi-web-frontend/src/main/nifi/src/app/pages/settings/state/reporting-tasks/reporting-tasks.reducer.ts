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

import { createReducer, on } from '@ngrx/store';
import { ReportingTasksState } from './index';
import {
    configureReportingTask,
    configureReportingTaskSuccess,
    createReportingTask,
    createReportingTaskSuccess,
    deleteReportingTask,
    deleteReportingTaskSuccess,
    loadReportingTasks,
    loadReportingTasksSuccess,
    reportingTasksBannerApiError,
    reportingTasksSnackbarApiError,
    resetReportingTasksState,
    startReportingTaskSuccess,
    stopReportingTaskSuccess
} from './reporting-tasks.actions';
import { produce } from 'immer';

export const initialState: ReportingTasksState = {
    reportingTasks: [],
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const reportingTasksReducer = createReducer(
    initialState,
    on(resetReportingTasksState, () => ({
        ...initialState
    })),
    on(loadReportingTasks, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadReportingTasksSuccess, (state, { response }) => ({
        ...state,
        reportingTasks: response.reportingTasks,
        loadedTimestamp: response.loadedTimestamp,
        status: 'success' as const
    })),
    on(reportingTasksBannerApiError, reportingTasksSnackbarApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(configureReportingTaskSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.reportingTasks.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.reportingTasks[componentIndex] = response.reportingTask;
            }
            draftState.saving = false;
        });
    }),
    on(createReportingTask, deleteReportingTask, configureReportingTask, (state) => ({
        ...state,
        saving: true
    })),
    on(createReportingTaskSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.reportingTasks.push(response.reportingTask);
            draftState.saving = false;
        });
    }),
    on(startReportingTaskSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.reportingTasks.findIndex(
                (f: any) => response.reportingTask.id === f.id
            );
            if (componentIndex > -1) {
                draftState.reportingTasks[componentIndex] = response.reportingTask;
            }
        });
    }),
    on(stopReportingTaskSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.reportingTasks.findIndex(
                (f: any) => response.reportingTask.id === f.id
            );
            if (componentIndex > -1) {
                draftState.reportingTasks[componentIndex] = response.reportingTask;
            }
        });
    }),
    on(deleteReportingTaskSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.reportingTasks.findIndex(
                (f: any) => response.reportingTask.id === f.id
            );
            if (componentIndex > -1) {
                draftState.reportingTasks.splice(componentIndex, 1);
            }
            draftState.saving = false;
        });
    })
);
