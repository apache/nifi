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

import { createSelector } from '@ngrx/store';
import { selectSettingsState, SettingsState } from '../index';
import { ParameterProviderEntity, parameterProvidersFeatureKey, ParameterProvidersState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectParameterProvidersState = createSelector(
    selectSettingsState,
    (state: SettingsState) => state[parameterProvidersFeatureKey]
);

export const selectSaving = createSelector(
    selectParameterProvidersState,
    (state: ParameterProvidersState) => state.saving
);

export const selectStatus = createSelector(
    selectParameterProvidersState,
    (state: ParameterProvidersState) => state.status
);

export const selectParameterProviderIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the parameter provider from the route
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedParameterProvider = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectSingleFetchParameterProvider = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'fetch') {
        return route.params.id;
    }
    return null;
});

export const selectParameterProviders = createSelector(
    selectParameterProvidersState,
    (state: ParameterProvidersState) => state.parameterProviders
);

export const selectParameterProvider = (id: string) =>
    createSelector(selectParameterProviders, (entities: ParameterProviderEntity[]) =>
        entities.find((entity) => id === entity.id)
    );

export const selectApplyParameterProviderParametersRequest = createSelector(
    selectParameterProvidersState,
    (state: ParameterProvidersState) => state.applyParametersRequestEntity
);
