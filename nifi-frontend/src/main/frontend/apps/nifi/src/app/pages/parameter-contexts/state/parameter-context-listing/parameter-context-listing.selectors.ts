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
import { ParameterContextsState, selectParameterContextState } from '../index';
import { parameterContextListingFeatureKey, ParameterContextListingState } from './index';
import { selectCurrentRoute } from '@nifi/shared';
import { ParameterContextEntity } from '../../../../state/shared';

export const selectParameterContextListingState = createSelector(
    selectParameterContextState,
    (state: ParameterContextsState) => state[parameterContextListingFeatureKey]
);

export const selectSaving = createSelector(
    selectParameterContextListingState,
    (state: ParameterContextListingState) => state.saving
);

export const selectParameterContextIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedParameterContext = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectUpdateRequest = createSelector(
    selectParameterContextListingState,
    (state: ParameterContextListingState) => state.updateRequestEntity
);

export const selectParameterContexts = createSelector(
    selectParameterContextListingState,
    (state: ParameterContextListingState) => state.parameterContexts
);

export const selectParameterContextStatus = createSelector(
    selectParameterContextListingState,
    (state: ParameterContextListingState) => state.status
);

export const selectContext = (id: string) =>
    createSelector(selectParameterContexts, (parameterContexts: ParameterContextEntity[]) =>
        parameterContexts.find((entity) => id == entity.id)
    );
