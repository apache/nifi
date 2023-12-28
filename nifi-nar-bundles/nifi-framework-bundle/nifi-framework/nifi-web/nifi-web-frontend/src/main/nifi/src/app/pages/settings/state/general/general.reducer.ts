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
    updateControllerConfigSuccess
} from './general.actions';
import { Revision } from '../../../../state/shared';

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
    error: null,
    status: 'pending'
};

export const generalReducer = createReducer(
    initialState,
    on(resetGeneralState, (state) => ({
        ...initialState
    })),
    on(loadControllerConfig, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadControllerConfigSuccess, (state, { response }) => ({
        ...state,
        controller: response.controller,
        error: null,
        status: 'success' as const
    })),
    on(controllerConfigApiError, (state, { error }) => ({
        ...state,
        error,
        status: 'error' as const
    })),
    on(updateControllerConfigSuccess, (state, { response }) => ({
        ...state,
        controller: response.controller,
        error: null,
        status: 'success' as const
    }))
);
