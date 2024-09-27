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

import { createReducer, on } from '@ngrx/store';
import {
    loadParameterProviderDefinition,
    loadParameterProviderDefinitionSuccess,
    parameterProviderDefinitionApiError,
    resetParameterProviderDefinitionState
} from './parameter-provider-definition.actions';
import { ParameterProviderDefinitionState } from './index';

export const initialState: ParameterProviderDefinitionState = {
    parameterProviderDefinition: null,
    error: null,
    status: 'pending'
};

export const parameterProviderDefinitionReducer = createReducer(
    initialState,
    on(loadParameterProviderDefinition, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadParameterProviderDefinitionSuccess, (state, { parameterProviderDefinition }) => ({
        ...state,
        parameterProviderDefinition,
        error: null,
        status: 'success' as const
    })),
    on(parameterProviderDefinitionApiError, (state, { error }) => ({
        ...state,
        parameterProviderDefinition: null,
        error,
        status: 'error' as const
    })),
    on(resetParameterProviderDefinitionState, () => ({
        ...initialState
    }))
);
