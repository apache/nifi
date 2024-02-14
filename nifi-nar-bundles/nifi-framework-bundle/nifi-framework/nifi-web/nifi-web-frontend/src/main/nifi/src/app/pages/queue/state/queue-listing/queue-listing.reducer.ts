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
    loadConnectionLabelSuccess,
    deleteQueueListingRequestSuccess
} from './queue-listing.actions';
import { produce } from 'immer';

export const initialState: QueueListingState = {
    activeListingRequest: null,
    completedListingRequest: {
        id: '',
        uri: '',
        submissionTime: '',
        lastUpdated: '',
        percentCompleted: 100,
        finished: true,
        failureReason: '',
        maxResults: 0,
        sourceRunning: false,
        destinationRunning: false,
        state: '',
        queueSize: {
            objectCount: 0,
            byteCount: 0
        },
        flowFileSummaries: []
    },
    connectionLabel: 'Connection',
    loadedTimestamp: 'N/A',
    status: 'pending'
};

export const queueListingReducer = createReducer(
    initialState,
    on(loadConnectionLabelSuccess, (state, { response }) => ({
        ...state,
        connectionLabel: response.connectionLabel
    })),
    on(submitQueueListingRequest, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(submitQueueListingRequestSuccess, pollQueueListingRequestSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const listingRequest = response.requestEntity.listingRequest;
            draftState.activeListingRequest = listingRequest;

            // if the query has finished save it as completed, the active query will be reset after deletion
            if (listingRequest.finished) {
                draftState.completedListingRequest = listingRequest;
                draftState.loadedTimestamp = listingRequest.lastUpdated;
                draftState.status = 'success' as const;
            }
        });
    }),
    on(deleteQueueListingRequestSuccess, (state) => ({
        ...state,
        activeListingRequest: null
    })),
    on(queueListingApiError, (state) => ({
        ...state,
        status: 'error' as const
    })),
    on(resetQueueListingState, () => ({
        ...initialState
    }))
);
