/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { createSelector } from '@ngrx/store';
import { selectSummaryState, SummaryState } from '../index';
import { ProcessorStatusSnapshotEntity, summaryListingFeatureKey, SummaryListingState } from './index';
import { selectCurrentRoute } from '../../../../state/router/router.selectors';

export const selectSummaryListing = createSelector(
    selectSummaryState,
    (state: SummaryState) => state[summaryListingFeatureKey]
);

export const selectSummaryListingLoadedTimestamp = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.loadedTimestamp
);

export const selectSummaryListingStatus = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.status
);

export const selectClusterSummary = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.clusterSummary
);

export const selectProcessGroupStatus = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.processGroupStatus
);

export const selectProcessorStatusSnapshots = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.processorStatusSnapshots
);

export const selectProcessorStatus = (id: string) =>
    createSelector(selectProcessorStatusSnapshots, (processors: ProcessorStatusSnapshotEntity[]) =>
        processors.find((processor) => id === processor.id)
    );

export const selectProcessorIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectViewStatusHistory = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path === 'history') {
        return route.params.id;
    }
});
