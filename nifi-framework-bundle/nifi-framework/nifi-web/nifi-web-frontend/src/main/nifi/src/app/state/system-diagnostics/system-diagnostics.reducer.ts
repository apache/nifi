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

import { SystemDiagnosticsState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    getSystemDiagnosticsAndOpenDialog,
    loadSystemDiagnosticsSuccess,
    reloadSystemDiagnostics,
    reloadSystemDiagnosticsSuccess,
    resetSystemDiagnostics,
    systemDiagnosticsBannerError,
    systemDiagnosticsSnackbarError,
    viewSystemDiagnosticsComplete
} from './system-diagnostics.actions';

export const initialSystemDiagnosticsState: SystemDiagnosticsState = {
    systemDiagnostics: null,
    status: 'pending',
    loadedTimestamp: ''
};

export const systemDiagnosticsReducer = createReducer(
    initialSystemDiagnosticsState,

    on(reloadSystemDiagnostics, getSystemDiagnosticsAndOpenDialog, (state) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadSystemDiagnosticsSuccess, reloadSystemDiagnosticsSuccess, (state, { response }) => ({
        ...state,
        status: 'success' as const,
        loadedTimestamp: response.systemDiagnostics.aggregateSnapshot.statsLastRefreshed,
        systemDiagnostics: response.systemDiagnostics
    })),

    on(systemDiagnosticsBannerError, systemDiagnosticsSnackbarError, (state) => ({
        ...state,
        status: 'error' as const
    })),

    on(resetSystemDiagnostics, () => ({
        ...initialSystemDiagnosticsState
    })),

    on(viewSystemDiagnosticsComplete, () => ({
        ...initialSystemDiagnosticsState
    }))
);
