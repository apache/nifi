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
import { connectorsListingFeatureKey, ConnectorsListingState, selectConnectorsState } from '../index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectConnectorsListingState = createSelector(
    selectConnectorsState,
    (state) => state[connectorsListingFeatureKey]
);

export const selectConnectors = createSelector(
    selectConnectorsListingState,
    (state: ConnectorsListingState) => state.connectors
);

export const selectSaving = createSelector(
    selectConnectorsListingState,
    (state: ConnectorsListingState) => state.saving
);

export const selectStatus = createSelector(
    selectConnectorsListingState,
    (state: ConnectorsListingState) => state.status
);

export const selectLoadedTimestamp = createSelector(
    selectConnectorsListingState,
    (state: ConnectorsListingState) => state.loadedTimestamp
);

export const selectConnectorIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params['id'];
    }
    return null;
});
