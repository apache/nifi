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
import { selectJoltTransformState, JoltTransformState, joltTransformJsonUiFeatureKey } from '../index';
import { JoltTransformJsonUiState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectJoltTransformJsonUiState = createSelector(
    selectJoltTransformState,
    (state: JoltTransformState) => state[joltTransformJsonUiFeatureKey]
);

export const selectProcessorDetails = createSelector(
    selectJoltTransformJsonUiState,
    (state: JoltTransformJsonUiState) => state.processorDetails
);

export const selectSaving = createSelector(
    selectJoltTransformJsonUiState,
    (state: JoltTransformJsonUiState) => state.saving
);

export const selectStatus = createSelector(
    selectJoltTransformJsonUiState,
    (state: JoltTransformJsonUiState) => state.status
);

export const selectProcessorIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.queryParams.id;
    }
    return null;
});

export const selectRevisionFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.queryParams.revision;
    }
    return null;
});

export const selectClientIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.queryParams.clientId;
    }
    return null;
});

export const selectEditableFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.queryParams.editable;
    }
    return null;
});

export const selectDisconnectedNodeAcknowledgedFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.queryParams.disconnectedNodeAcknowledged;
    }
    return null;
});
