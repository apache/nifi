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

import { createFeatureSelector, createSelector } from '@ngrx/store';
import { selectCurrentRoute } from '@nifi/shared';
import { remotePortsFeatureKey, RemotePortsState } from './index';

export const selectRemotePortsState = createFeatureSelector<RemotePortsState>(remotePortsFeatureKey);

export const selectSaving = createSelector(selectRemotePortsState, (state: RemotePortsState) => state.saving);

export const selectStatus = createSelector(selectRemotePortsState, (state: RemotePortsState) => state.status);

export const selectRpgIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the rpg id from the route
        return route.params.rpgId;
    }
    return null;
});

export const selectPortIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the port id from the route
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedPort = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectPorts = createSelector(selectRemotePortsState, (state: RemotePortsState) => state.ports);
export const selectRpg = createSelector(selectRemotePortsState, (state: RemotePortsState) => state.rpg);

export const selectPort = (id: string) =>
    createSelector(selectPorts, (port: any[]) => port.find((port) => id == port.id));
