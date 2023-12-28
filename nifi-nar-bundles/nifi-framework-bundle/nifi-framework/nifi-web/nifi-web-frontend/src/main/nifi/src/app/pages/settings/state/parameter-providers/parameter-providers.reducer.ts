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

import { ParameterProvidersState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    configureParameterProvider,
    configureParameterProviderSuccess,
    createParameterProvider,
    createParameterProviderSuccess,
    deleteParameterProvider,
    deleteParameterProviderSuccess,
    loadParameterProviders,
    loadParameterProvidersSuccess,
    parameterProvidersApiError,
    resetParameterProvidersState
} from './parameter-providers.actions';
import { produce } from 'immer';

export const initialParameterProvidersState: ParameterProvidersState = {
    parameterProviders: [],
    saving: false,
    loadedTimestamp: '',
    error: null,
    status: 'pending'
};

export const parameterProvidersReducer = createReducer(
    initialParameterProvidersState,

    on(resetParameterProvidersState, () => ({
        ...initialParameterProvidersState
    })),

    on(loadParameterProviders, (state: ParameterProvidersState) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadParameterProvidersSuccess, (state: ParameterProvidersState, { response }) => ({
        ...state,
        parameterProviders: response.parameterProviders,
        loadedTimestamp: response.loadedTimestamp,
        error: null,
        status: 'success' as const
    })),

    on(parameterProvidersApiError, (state: ParameterProvidersState, { error }) => ({
        ...state,
        saving: false,
        error,
        status: 'error' as const
    })),

    on(createParameterProvider, configureParameterProvider, deleteParameterProvider, (state) => ({
        ...state,
        saving: true
    })),

    on(createParameterProviderSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.parameterProviders.push(response.parameterProvider);
            draftState.saving = false;
        });
    }),

    on(deleteParameterProviderSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const idx = draftState.parameterProviders.findIndex(
                (provider) => provider.id === response.parameterProvider.id
            );
            if (idx > -1) {
                draftState.parameterProviders.splice(idx, 1);
            }
            draftState.saving = false;
        });
    }),

    on(configureParameterProviderSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const idx = draftState.parameterProviders.findIndex(
                (provider) => provider.id === response.parameterProvider.id
            );

            if (idx > -1) {
                draftState.parameterProviders[idx] = response.parameterProvider;
            }

            draftState.saving = false;
        });
    })
);
