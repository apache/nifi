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
import { initialConnectorControllerServicesState } from './index';
import {
    loadConnectorControllerServices,
    loadConnectorControllerServicesSuccess,
    loadConnectorControllerServicesFailure,
    resetConnectorControllerServicesState
} from './connector-controller-services.actions';

export const connectorControllerServicesReducer = createReducer(
    initialConnectorControllerServicesState,

    on(loadConnectorControllerServices, (state, { request }) => ({
        ...state,
        connectorId: request.connectorId,
        processGroupId: request.processGroupId,
        status: 'loading' as const,
        error: null
    })),

    on(loadConnectorControllerServicesSuccess, (state, { response }) => ({
        ...state,
        connectorId: response.connectorId,
        processGroupId: response.processGroupId,
        breadcrumb: response.breadcrumb,
        controllerServices: response.controllerServices,
        loadedTimestamp: response.loadedTimestamp,
        status: 'success' as const,
        error: null,
        hasAttemptedLoad: true
    })),

    on(loadConnectorControllerServicesFailure, (state, { errorContext }) => ({
        ...state,
        status: 'error' as const,
        error: errorContext.errors[0] ?? 'Failed to load controller services',
        hasAttemptedLoad: true
    })),

    on(resetConnectorControllerServicesState, () => initialConnectorControllerServicesState)
);
