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
import { BulkReplayStatusState, selectBulkReplayStatusState } from './index';

export const selectJobs = createSelector(selectBulkReplayStatusState, (state: BulkReplayStatusState) => state.jobs);

export const selectJobItems = createSelector(
    selectBulkReplayStatusState,
    (state: BulkReplayStatusState) => state.jobItems
);

export const selectLoadedTimestamp = createSelector(
    selectBulkReplayStatusState,
    (state: BulkReplayStatusState) => state.loadedTimestamp
);

export const selectLoading = createSelector(
    selectBulkReplayStatusState,
    (state: BulkReplayStatusState) => state.loading
);

export const selectConfig = createSelector(selectBulkReplayStatusState, (state: BulkReplayStatusState) => state.config);
