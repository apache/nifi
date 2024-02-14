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
import { FlowConfigurationHistoryState, selectFlowConfigurationHistoryState } from '../index';
import { flowConfigurationHistoryListingFeatureKey, FlowConfigurationHistoryListingState } from './index';

export const selectFlowConfigurationHistoryListingState = createSelector(
    selectFlowConfigurationHistoryState,
    (state: FlowConfigurationHistoryState) => state[flowConfigurationHistoryListingFeatureKey]
);

export const selectHistoryActions = createSelector(
    selectFlowConfigurationHistoryListingState,
    (state: FlowConfigurationHistoryListingState) => state.actions
);

export const selectHistoryStatus = createSelector(
    selectFlowConfigurationHistoryListingState,
    (state: FlowConfigurationHistoryListingState) => state.status
);

export const selectHistoryLoadedTimestamp = createSelector(
    selectFlowConfigurationHistoryListingState,
    (state: FlowConfigurationHistoryListingState) => state.loadedTimestamp
);

export const selectHistoryQuery = createSelector(
    selectFlowConfigurationHistoryListingState,
    (state: FlowConfigurationHistoryListingState) => state.query
);

export const selectHistoryTotalResults = createSelector(
    selectFlowConfigurationHistoryListingState,
    (state: FlowConfigurationHistoryListingState) => state.total
);

export const selectedHistoryItem = createSelector(
    selectFlowConfigurationHistoryListingState,
    (state: FlowConfigurationHistoryListingState) => state.selectedId
);
