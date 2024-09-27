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

import { ReportingTaskDefinitionState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    loadReportingTaskDefinition,
    loadReportingTaskDefinitionSuccess,
    reportingTaskDefinitionApiError,
    resetReportingTaskDefinitionState
} from './reporting-task-definition.actions';

export const initialState: ReportingTaskDefinitionState = {
    reportingTaskDefinition: null,
    error: null,
    status: 'pending'
};

export const reportingTaskDefinitionReducer = createReducer(
    initialState,
    on(loadReportingTaskDefinition, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadReportingTaskDefinitionSuccess, (state, { reportingTaskDefinition }) => ({
        ...state,
        reportingTaskDefinition,
        error: null,
        status: 'success' as const
    })),
    on(reportingTaskDefinitionApiError, (state, { error }) => ({
        ...state,
        reportingTaskDefinition: null,
        error,
        status: 'error' as const
    })),
    on(resetReportingTaskDefinitionState, () => ({
        ...initialState
    }))
);
