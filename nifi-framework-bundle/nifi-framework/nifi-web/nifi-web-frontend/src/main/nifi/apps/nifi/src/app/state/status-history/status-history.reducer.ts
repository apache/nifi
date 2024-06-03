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

import { StatusHistoryEntity, StatusHistoryState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    clearStatusHistory,
    getStatusHistoryAndOpenDialog,
    loadStatusHistorySuccess,
    reloadStatusHistory,
    reloadStatusHistorySuccess,
    statusHistoryBannerError,
    viewNodeStatusHistoryComplete,
    viewStatusHistoryComplete
} from './status-history.actions';

export const initialState: StatusHistoryState = {
    statusHistory: {} as StatusHistoryEntity,
    status: 'pending',
    loadedTimestamp: ''
};

export const statusHistoryReducer = createReducer(
    initialState,

    on(reloadStatusHistory, getStatusHistoryAndOpenDialog, (state) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadStatusHistorySuccess, reloadStatusHistorySuccess, (state, { response }) => ({
        ...state,
        status: 'success' as const,
        loadedTimestamp: response.statusHistory.statusHistory.generated,
        statusHistory: response.statusHistory
    })),

    on(statusHistoryBannerError, (state) => ({
        ...state,
        status: 'error' as const
    })),

    on(clearStatusHistory, () => ({
        ...initialState
    })),

    on(viewStatusHistoryComplete, viewNodeStatusHistoryComplete, () => ({
        ...initialState
    }))
);
