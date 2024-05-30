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
import { produce } from 'immer';
import { RegistryClientsState } from './index';
import {
    configureRegistryClient,
    configureRegistryClientSuccess,
    createRegistryClient,
    createRegistryClientSuccess,
    deleteRegistryClient,
    deleteRegistryClientSuccess,
    loadRegistryClients,
    loadRegistryClientsSuccess,
    registryClientsBannerApiError,
    registryClientsSnackbarApiError,
    resetRegistryClientsState
} from './registry-clients.actions';

export const initialState: RegistryClientsState = {
    registryClients: [],
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const registryClientsReducer = createReducer(
    initialState,
    on(resetRegistryClientsState, () => ({
        ...initialState
    })),
    on(loadRegistryClients, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadRegistryClientsSuccess, (state, { response }) => ({
        ...state,
        registryClients: response.registryClients,
        loadedTimestamp: response.loadedTimestamp,
        status: 'success' as const
    })),
    on(registryClientsBannerApiError, registryClientsSnackbarApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(createRegistryClient, configureRegistryClient, deleteRegistryClient, (state) => ({
        ...state,
        saving: true
    })),
    on(createRegistryClientSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.registryClients.push(response.registryClient);
            draftState.saving = false;
        });
    }),
    on(configureRegistryClientSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.registryClients.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.registryClients[componentIndex] = response.registryClient;
            }
            draftState.saving = false;
        });
    }),
    on(deleteRegistryClientSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.registryClients.findIndex(
                (f: any) => response.registryClient.id === f.id
            );
            if (componentIndex > -1) {
                draftState.registryClients.splice(componentIndex, 1);
            }
            draftState.saving = false;
        });
    })
);
