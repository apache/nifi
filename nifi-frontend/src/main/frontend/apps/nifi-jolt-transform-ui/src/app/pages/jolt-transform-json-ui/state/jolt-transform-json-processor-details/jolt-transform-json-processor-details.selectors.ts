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
import { selectCurrentRoute } from '@nifi/shared';
import {
    joltTransformJsonProcessorDetailsFeatureKey,
    JoltTransformJsonUiState,
    selectJoltTransformJsonUiState
} from '../index';
import { JoltTransformJsonProcessorDetailsState } from './index';

export const selectJoltTransformJsonProcessorDetailsState = createSelector(
    selectJoltTransformJsonUiState,
    (state: JoltTransformJsonUiState) => state[joltTransformJsonProcessorDetailsFeatureKey]
);

export const selectProcessorDetails = createSelector(
    selectJoltTransformJsonProcessorDetailsState,
    (state: JoltTransformJsonProcessorDetailsState) => state.processorDetails
);

export const selectProcessorDetailsLoading = createSelector(
    selectJoltTransformJsonProcessorDetailsState,
    (state: JoltTransformJsonProcessorDetailsState) => state.loading
);

export const selectProcessorDetailsError = createSelector(
    selectJoltTransformJsonProcessorDetailsState,
    (state: JoltTransformJsonProcessorDetailsState) => state.error
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
