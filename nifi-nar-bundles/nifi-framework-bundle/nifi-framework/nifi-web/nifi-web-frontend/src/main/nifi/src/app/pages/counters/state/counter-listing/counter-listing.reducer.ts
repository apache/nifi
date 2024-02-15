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

import { CounterListingState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    counterListingApiError,
    loadCounters,
    loadCountersSuccess,
    resetCounterState,
    resetCounterSuccess
} from './counter-listing.actions';
import { produce } from 'immer';

export const initialState: CounterListingState = {
    counters: [],
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const counterListingReducer = createReducer(
    initialState,
    on(loadCounters, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadCountersSuccess, (state, { response }) => ({
        ...state,
        counters: response.counters,
        loadedTimestamp: response.loadedTimestamp,
        status: 'success' as const
    })),
    on(counterListingApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(resetCounterSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const index: number = draftState.counters.findIndex((c: any) => c.id === response.counter.id);
            if (index > -1) {
                draftState.counters[index] = {
                    ...response.counter
                };
            }
        });
    }),
    on(resetCounterState, () => ({
        ...initialState
    }))
);
