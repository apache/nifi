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

import { ControllerServiceDefinitionState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    loadControllerServiceDefinition,
    loadControllerServiceDefinitionSuccess,
    controllerServiceDefinitionApiError,
    resetControllerServiceDefinitionState
} from './controller-service-definition.actions';

export const initialState: ControllerServiceDefinitionState = {
    controllerServiceDefinition: null,
    error: null,
    status: 'pending'
};

export const controllerServiceDefinitionReducer = createReducer(
    initialState,
    on(loadControllerServiceDefinition, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadControllerServiceDefinitionSuccess, (state, { controllerServiceDefinition }) => ({
        ...state,
        controllerServiceDefinition,
        error: null,
        status: 'success' as const
    })),
    on(controllerServiceDefinitionApiError, (state, { error }) => ({
        ...state,
        controllerServiceDefinition: null,
        error,
        status: 'error' as const
    })),
    on(resetControllerServiceDefinitionState, () => ({
        ...initialState
    }))
);
