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

import { createReducer, on } from '@ngrx/store';
import { QueueListingState } from './index';
import {
    pollQueueListingRequestSuccess,
    submitQueueListingRequest,
    submitQueueListingRequestSuccess,
    resetQueueListingState,
    queueListingApiError,
    loadConnectionLabelSuccess
} from './queue-listing.actions';

export const initialState: QueueListingState = {
    requestEntity: null,
    connectionLabel: 'Connection',
    loadedTimestamp: 'N/A',
    error: null,
    status: 'pending'
};

export const queueListingReducer = createReducer(
    initialState,
    on(loadConnectionLabelSuccess, (state, { response }) => ({
        ...state,
        connectionLabel: response.connectionLabel
    })),
    on(submitQueueListingRequest, (state, { request }) => ({
        ...state,
        status: 'loading' as const
    })),
    on(submitQueueListingRequestSuccess, pollQueueListingRequestSuccess, (state, { response }) => ({
        ...state,
        requestEntity: response.requestEntity,
        loadedTimestamp: response.requestEntity.listingRequest.lastUpdated,
        error: null,
        status: 'success' as const
    })),
    on(queueListingApiError, (state, { error }) => ({
        ...state,
        error,
        status: 'error' as const
    })),
    on(resetQueueListingState, (state) => ({
        ...initialState
    }))
);
