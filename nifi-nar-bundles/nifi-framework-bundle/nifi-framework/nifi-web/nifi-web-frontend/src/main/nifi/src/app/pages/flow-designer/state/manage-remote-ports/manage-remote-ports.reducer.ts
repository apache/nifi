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
import {
    configureRemotePort,
    configureRemotePortSuccess,
    loadRemotePorts,
    loadRemotePortsSuccess,
    remotePortsBannerApiError,
    resetRemotePortsState
} from './manage-remote-ports.actions';
import { produce } from 'immer';
import { RemotePortsState } from './index';

export const initialState: RemotePortsState = {
    ports: [],
    saving: false,
    loadedTimestamp: '',
    rpg: null,
    status: 'pending'
};

export const manageRemotePortsReducer = createReducer(
    initialState,
    on(resetRemotePortsState, () => ({
        ...initialState
    })),
    on(loadRemotePorts, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadRemotePortsSuccess, (state, { response }) => ({
        ...state,
        ports: response.ports,
        loadedTimestamp: response.loadedTimestamp,
        rpg: response.rpg,
        status: 'success' as const
    })),
    on(remotePortsBannerApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(configureRemotePort, (state) => ({
        ...state,
        saving: true
    })),
    on(configureRemotePortSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.ports.findIndex((f: any) => response.id === f.id);
            const port = {
                ...response.port,
                type: state.ports[componentIndex].type
            };
            if (componentIndex > -1) {
                draftState.ports[componentIndex] = port;
            }
            draftState.saving = false;
        });
    })
);
