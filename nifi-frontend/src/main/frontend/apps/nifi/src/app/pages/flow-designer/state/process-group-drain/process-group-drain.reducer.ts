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
import { DrainProcessGroupState } from './index';
import {
    beginDrain,
    beginDrainSuccess,
    cancelDrain,
    cancelDrainSuccess,
    drainApiError,
    drainEnded,
    pollDrainSuccess,
    resetDrainState,
    viewDrain
} from './process-group-drain.actions';

export const initialState: DrainProcessGroupState = {
    processGroupId: null,
    drainRequestId: null,
    drainRequest: null,
    status: 'idle',
    error: null
};

export const processGroupDrainReducer = createReducer(
    initialState,
    on(beginDrain, (state, { request }) => ({
        ...initialState,
        processGroupId: request.processGroupId,
        status: 'starting' as const
    })),
    on(viewDrain, (state, { request }) => ({
        ...initialState,
        processGroupId: request.processGroupId,
        drainRequestId: request.drainRequestId,
        status: 'draining' as const
    })),
    on(beginDrainSuccess, pollDrainSuccess, (state, { response }) => ({
        ...state,
        drainRequestId: response.drainRequestEntity.drainRequest?.id ?? state.drainRequestId,
        drainRequest: response.drainRequestEntity.drainRequest ?? null,
        status: 'draining' as const,
        error: null
    })),
    on(cancelDrain, (state) => ({
        ...state,
        status: 'ending' as const
    })),
    on(cancelDrainSuccess, (state, { response }) => ({
        ...state,
        drainRequest: response.drainRequestEntity.drainRequest ?? null,
        status: 'ended' as const
    })),
    on(drainEnded, (state) => ({
        ...state,
        status: 'ended' as const
    })),
    on(drainApiError, (state, { error }) => ({
        ...initialState,
        error
    })),
    on(resetDrainState, () => ({
        ...initialState
    }))
);
