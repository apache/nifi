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
import { managementControllerServicesFeatureKey, ManagementControllerServicesState } from './index';
import { selectCurrentRoute } from '@nifi/shared';
import { ControllerServiceEntity } from '../../../../state/shared';

export const selectManagementControllerServicesState = createSelector(
    selectSettingsState,
    (state: SettingsState) => state[managementControllerServicesFeatureKey]
);

export const selectSaving = createSelector(
    selectManagementControllerServicesState,
    (state: ManagementControllerServicesState) => state.saving
);

export const selectStatus = createSelector(
    selectManagementControllerServicesState,
    (state: ManagementControllerServicesState) => state.status
);

export const selectControllerServiceIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the controller service from the route
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedService = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectServices = createSelector(
    selectManagementControllerServicesState,
    (state: ManagementControllerServicesState) => state.controllerServices
);

export const selectService = (id: string) =>
    createSelector(selectServices, (services: ControllerServiceEntity[]) =>
        services.find((service) => id == service.id)
    );
