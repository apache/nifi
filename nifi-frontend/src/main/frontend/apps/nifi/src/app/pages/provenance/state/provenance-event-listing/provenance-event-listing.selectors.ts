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
import { ProvenanceState, selectProvenanceState } from '../index';
import {
    Provenance,
    provenanceEventListingFeatureKey,
    ProvenanceEventListingState,
    ProvenanceQueryParams,
    ProvenanceResults
} from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectProvenanceEventListingState = createSelector(
    selectProvenanceState,
    (state: ProvenanceState) => state[provenanceEventListingFeatureKey]
);

export const selectSearchableFieldsFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        const queryParameters: ProvenanceQueryParams = {};
        if (route.queryParams.componentId) {
            queryParameters.componentId = route.queryParams.componentId;
        }
        if (route.queryParams.flowFileUuid) {
            queryParameters.flowFileUuid = route.queryParams.flowFileUuid;
        }
        return queryParameters;
    }
    return null;
});

export const selectProvenanceRequest = createSelector(
    selectProvenanceEventListingState,
    (state: ProvenanceEventListingState) => state.request
);

export const selectStatus = createSelector(
    selectProvenanceEventListingState,
    (state: ProvenanceEventListingState) => state.status
);

export const selectProvenanceOptions = createSelector(
    selectProvenanceEventListingState,
    (state: ProvenanceEventListingState) => state.options
);

export const selectLoadedTimestamp = createSelector(
    selectProvenanceEventListingState,
    (state: ProvenanceEventListingState) => state.loadedTimestamp
);

export const selectActiveProvenance = createSelector(
    selectProvenanceEventListingState,
    (state: ProvenanceEventListingState) => state.activeProvenance
);

export const selectCompletedProvenance = createSelector(
    selectProvenanceEventListingState,
    (state: ProvenanceEventListingState) => state.completedProvenance
);

export const selectActiveProvenanceId = createSelector(selectActiveProvenance, (state: Provenance | null) => state?.id);

export const selectClusterNodeIdFromActiveProvenance = createSelector(
    selectActiveProvenance,
    (state: Provenance | null) => state?.request.clusterNodeId
);

export const selectProvenanceResults = createSelector(
    selectCompletedProvenance,
    (state: Provenance | null) => state?.results
);

export const selectTimeOffset = createSelector(
    selectProvenanceResults,
    (state: ProvenanceResults | undefined) => state?.timeOffset
);
