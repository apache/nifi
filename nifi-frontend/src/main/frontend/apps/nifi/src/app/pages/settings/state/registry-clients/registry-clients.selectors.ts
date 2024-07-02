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

import { createSelector } from '@ngrx/store';
import { selectSettingsState, SettingsState } from '../index';
import { selectCurrentRoute } from '@nifi/shared';
import { registryClientsFeatureKey, RegistryClientsState } from './index';
import { RegistryClientEntity } from '../../../../state/shared';

export const selectRegistryClientsState = createSelector(
    selectSettingsState,
    (state: SettingsState) => state[registryClientsFeatureKey]
);

export const selectSaving = createSelector(selectRegistryClientsState, (state: RegistryClientsState) => state.saving);

export const selectStatus = createSelector(selectRegistryClientsState, (state: RegistryClientsState) => state.status);

export const selectRegistryClientIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the registry client from the route
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedRegistryClient = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectRegistryClients = createSelector(
    selectRegistryClientsState,
    (state: RegistryClientsState) => state.registryClients
);

export const selectRegistryClient = (id: string) =>
    createSelector(selectRegistryClients, (entities: RegistryClientEntity[]) =>
        entities.find((entity) => id == entity.id)
    );
