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
import { QueueState } from './index';
import {
    pollEmptyQueueRequestSuccess,
    submitEmptyQueueRequest,
    submitEmptyQueueRequestSuccess,
    resetQueueState,
    submitEmptyQueuesRequest
} from './queue.actions';

export const initialState: QueueState = {
    dropEntity: null,
    processGroupId: null,
    connectionId: null,
    loadedTimestamp: 'N/A',
    status: 'pending'
};

export const queueReducer = createReducer(
    initialState,
    on(submitEmptyQueueRequest, (state, { request }) => ({
        ...state,
        connectionId: request.connectionId,
        status: 'loading' as const
    })),
    on(submitEmptyQueuesRequest, (state, { request }) => ({
        ...state,
        processGroupId: request.processGroupId,
        status: 'loading' as const
    })),
    on(submitEmptyQueueRequestSuccess, pollEmptyQueueRequestSuccess, (state, { response }) => ({
        ...state,
        dropEntity: response.dropEntity,
        loadedTimestamp: response.dropEntity.dropRequest.lastUpdated,
        status: 'success' as const
    })),
    on(resetQueueState, () => ({
        ...initialState
    }))
);
