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
import {
    ConnectionStatusSnapshotEntity,
    ProcessGroupStatusSnapshotEntity,
    ProcessorStatusSnapshotEntity,
    RemoteProcessGroupStatusSnapshotEntity,
    selectSummaryState,
    SummaryState
} from '../index';
import { summaryListingFeatureKey, SummaryListingState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

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

export const selectConnectionStatus = (id: string) =>
    createSelector(selectConnectionStatusSnapshots, (connections: ConnectionStatusSnapshotEntity[]) =>
        connections.find((connection) => id === connection.id)
    );

export const selectRemoteProcessGroupStatus = (id: string) =>
    createSelector(selectRemoteProcessGroupStatusSnapshots, (rpgs: RemoteProcessGroupStatusSnapshotEntity[]) =>
        rpgs.find((rpg) => id === rpg.id)
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

export const selectProcessGroupStatusSnapshots = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.processGroupStatusSnapshots
);

export const selectProcessGroupStatusItem = (id: string) =>
    createSelector(selectProcessGroupStatusSnapshots, (pgs: ProcessGroupStatusSnapshotEntity[]) =>
        pgs.find((pg) => id === pg.id)
    );

export const selectProcessGroupIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectInputPortIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectOutputPortIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectConnectionIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectRemoteProcessGroupIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectInputPortStatusSnapshots = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.inputPortStatusSnapshots
);

export const selectOutputPortStatusSnapshots = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.outputPortStatusSnapshots
);

export const selectConnectionStatusSnapshots = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.connectionStatusSnapshots
);

export const selectRemoteProcessGroupStatusSnapshots = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.remoteProcessGroupStatusSnapshots
);

export const selectSelectedClusterNode = createSelector(
    selectSummaryListing,
    (state: SummaryListingState) => state.selectedClusterNode
);
