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
import { selectSummaryState, SummaryState } from '../index';
import { componentClusterStatusFeatureKey, ComponentClusterStatusState } from './index';

export const selectComponentClusterStatusState = createSelector(
    selectSummaryState,
    (state: SummaryState) => state[componentClusterStatusFeatureKey]
);

export const selectComponentClusterStatusEntity = createSelector(
    selectComponentClusterStatusState,
    (state: ComponentClusterStatusState) => state.clusterStatus
);

export const selectComponentClusterStatusLatestRequest = createSelector(
    selectComponentClusterStatusState,
    (state: ComponentClusterStatusState) => state.latestRequest
);

export const selectComponentClusterStatusLoadingStatus = createSelector(
    selectComponentClusterStatusState,
    (state: ComponentClusterStatusState) => state.status
);

export const selectComponentClusterStatusLoadedTimestamp = createSelector(
    selectComponentClusterStatusState,
    (state: ComponentClusterStatusState) => state.loadedTimestamp
);
