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
import { EmptyQueueState } from './index';
import {
    pollEmptyQueueRequestSuccess,
    submitEmptyQueueRequest,
    submitEmptyQueueRequestSuccess,
    submitEmptyQueuesRequestSuccess,
    resetEmptyQueueState,
    submitEmptyQueuesRequest
} from './empty-queue.actions';

export const initialState: EmptyQueueState = {
    dropEntity: null,
    processGroupId: null,
    connectionId: null,
    source: null,
    loadedTimestamp: 'N/A',
    status: 'pending'
};

export const emptyQueueReducer = createReducer(
    initialState,
    // Explicitly null the unused identifier on each submit so the two
    // identifier fields remain mutually exclusive even if a prior request was
    // not explicitly reset.
    on(submitEmptyQueueRequest, (state, { request }) => ({
        ...state,
        connectionId: request.connectionId,
        processGroupId: null,
        source: request.source,
        status: 'loading' as const
    })),
    on(submitEmptyQueuesRequest, (state, { request }) => ({
        ...state,
        connectionId: null,
        processGroupId: request.processGroupId,
        source: request.source,
        status: 'loading' as const
    })),
    on(
        submitEmptyQueueRequestSuccess,
        submitEmptyQueuesRequestSuccess,
        pollEmptyQueueRequestSuccess,
        (state, { response }) => ({
            ...state,
            dropEntity: response.dropEntity,
            loadedTimestamp: response.dropEntity.dropRequest.lastUpdated,
            status: 'success' as const
        })
    ),
    on(resetEmptyQueueState, () => ({
        ...initialState
    }))
);
