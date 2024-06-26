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
import { queueListingFeatureKey, QueueListingState } from './index';
import { QueueState, selectQueueState } from '../index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectQueueListingState = createSelector(
    selectQueueState,
    (state: QueueState) => state[queueListingFeatureKey]
);

export const selectActiveListingRequest = createSelector(
    selectQueueListingState,
    (state: QueueListingState) => state.activeListingRequest
);

export const selectCompletedListingRequest = createSelector(
    selectQueueListingState,
    (state: QueueListingState) => state.completedListingRequest
);

export const selectStatus = createSelector(selectQueueListingState, (state: QueueListingState) => state.status);

export const selectSelectedConnection = createSelector(
    selectQueueListingState,
    (state: QueueListingState) => state.selectedConnection
);

export const selectLoadedTimestamp = createSelector(
    selectQueueListingState,
    (state: QueueListingState) => state.loadedTimestamp
);

export const selectConnectionIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route?.params.connectionId != null) {
        return route.params.connectionId;
    }
    return null;
});
