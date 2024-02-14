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

import { FlowConfigurationHistoryListingState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    clearHistorySelection,
    flowConfigurationHistorySnackbarError,
    loadHistory,
    loadHistorySuccess,
    purgeHistory,
    purgeHistorySuccess,
    resetHistoryState,
    selectHistoryItem
} from './flow-configuration-history-listing.actions';

export const initialHistoryState: FlowConfigurationHistoryListingState = {
    actions: [],
    total: 0,
    status: 'pending',
    loadedTimestamp: '',
    query: null,
    selectedId: null,
    purging: false
};

export const flowConfigurationHistoryListingReducer = createReducer(
    initialHistoryState,

    on(loadHistory, (state, { request }) => ({
        ...state,
        status: 'loading' as const,
        query: request
    })),

    on(loadHistorySuccess, (state, { response }) => ({
        ...state,
        status: 'success' as const,
        loadedTimestamp: response.history.lastRefreshed,
        actions: response.history.actions,
        total: response.history.total
    })),

    on(resetHistoryState, () => ({
        ...initialHistoryState
    })),

    on(purgeHistory, (state) => ({
        ...state,
        query: {
            ...state.query,
            count: 50,
            offset: 0
        },
        purging: true
    })),

    on(purgeHistorySuccess, flowConfigurationHistorySnackbarError, (state) => ({
        ...state,
        purging: false
    })),

    on(selectHistoryItem, (state, { request }) => ({
        ...state,
        selectedId: request.id
    })),

    on(clearHistorySelection, (state) => ({
        ...state,
        selectedId: null
    }))
);
