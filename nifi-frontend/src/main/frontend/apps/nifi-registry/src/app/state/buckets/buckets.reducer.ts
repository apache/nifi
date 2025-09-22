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
import { loadBuckets, loadBucketsSuccess } from './buckets.actions';
import { BucketsState } from '.';

export const initialState: BucketsState = {
    buckets: [],
    status: 'pending'
};

export const bucketsReducer = createReducer(
    initialState,
    on(loadBuckets, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadBucketsSuccess, (state, { response }) => ({
        ...state,
        buckets: response.buckets,
        status: 'success' as const
    }))
    // on(resetBuckets, () => ({
    //     ...initialState
    // }))
);
