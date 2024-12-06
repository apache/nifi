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
import { GeneralState, Controller } from './index';
import {
    controllerConfigApiError,
    loadControllerConfig,
    loadControllerConfigSuccess,
    resetGeneralState,
    updateControllerConfig,
    updateControllerConfigSuccess
} from './general.actions';
import { Revision } from '@nifi/shared';

export const INITIAL_REVISION: Revision = {
    version: 0,
    clientId: ''
};

export const INITIAL_CONTROLLER: Controller = {
    maxTimerDrivenThreadCount: 0
};

export const initialState: GeneralState = {
    controller: {
        revision: INITIAL_REVISION,
        component: INITIAL_CONTROLLER
    },
    saving: false,
    status: 'pending'
};

export const generalReducer = createReducer(
    initialState,
    on(resetGeneralState, () => ({
        ...initialState
    })),
    on(loadControllerConfig, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadControllerConfigSuccess, (state, { response }) => ({
        ...state,
        controller: response.controller,
        status: 'success' as const
    })),
    on(updateControllerConfig, (state) => ({
        ...state,
        saving: true
    })),
    on(controllerConfigApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(updateControllerConfigSuccess, (state, { response }) => ({
        ...state,
        saving: false,
        controller: response.controller,
        status: 'success' as const
    }))
);
