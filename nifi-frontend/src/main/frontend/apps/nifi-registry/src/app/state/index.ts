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

import { routerReducer, RouterReducerState, DEFAULT_ROUTER_FEATURENAME } from '@ngrx/router-store';
import { ActionReducerMap } from '@ngrx/store';
import { CurrentUserState, currentUserFeatureKey } from './current-user';
import { dropletsFeatureKey, DropletsState } from './droplets';
import { currentUserReducer } from './current-user/current-user.reducer';
import { dropletsReducer } from './droplets/droplets.reducer';
import { bucketsFeatureKey, BucketsState } from './buckets';
import { bucketsReducer } from './buckets/buckets.reducer';
import { errorReducer } from './error/error.reducer';
import { errorFeatureKey, ErrorState } from './error';

export interface NiFiState {
    [DEFAULT_ROUTER_FEATURENAME]: RouterReducerState;
    [errorFeatureKey]: ErrorState;
    [currentUserFeatureKey]: CurrentUserState;
    [dropletsFeatureKey]: DropletsState;
    [bucketsFeatureKey]: BucketsState;
}

export const rootReducers: ActionReducerMap<NiFiState> = {
    [DEFAULT_ROUTER_FEATURENAME]: routerReducer,
    [errorFeatureKey]: errorReducer,
    [currentUserFeatureKey]: currentUserReducer,
    [dropletsFeatureKey]: dropletsReducer,
    [bucketsFeatureKey]: bucketsReducer
};
