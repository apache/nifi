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
import { Action, ActionReducerMap, combineReducers } from '@ngrx/store';
import { dropletsFeatureKey, DropletsState } from './droplets';
import { dropletsReducer } from './droplets/droplets.reducer';
import { bucketsFeatureKey, BucketsState } from './buckets';
import { bucketsReducer } from './buckets/buckets.reducer';
import { policiesFeatureKey, PoliciesState } from './policies';
import { policiesReducer } from './policies/policies.reducer';
import { errorReducer } from './error/error.reducer';
import { errorFeatureKey, ErrorState } from './error';
import { aboutFeatureKey, AboutState } from './about';
import { aboutReducer } from './about/about.reducer';
import { currentUserFeatureKey, CurrentUserState } from './current-user';
import { currentUserReducer } from './current-user/current-user.reducer';

export const resourcesFeatureKey = 'resources';

export interface Params {
    rel: string;
}

export interface Link {
    href: string;
    params: Params;
}

export interface Revision {
    version: number;
}

export interface Permissions {
    canRead: boolean;
    canWrite: boolean;
    canDelete: boolean;
}

export interface ResourcesState {
    [dropletsFeatureKey]: DropletsState;
    [bucketsFeatureKey]: BucketsState;
    [policiesFeatureKey]: PoliciesState;
}

export function reducers(state: ResourcesState | undefined, action: Action) {
    return combineReducers({
        [dropletsFeatureKey]: dropletsReducer,
        [bucketsFeatureKey]: bucketsReducer,
        [policiesFeatureKey]: policiesReducer
    })(state, action);
}

export interface NiFiRegistryState {
    [DEFAULT_ROUTER_FEATURENAME]: RouterReducerState;
    [errorFeatureKey]: ErrorState;
    [aboutFeatureKey]: AboutState;
    [currentUserFeatureKey]: CurrentUserState;
}

export const rootReducers: ActionReducerMap<NiFiRegistryState> = {
    [DEFAULT_ROUTER_FEATURENAME]: routerReducer,
    [errorFeatureKey]: errorReducer,
    [aboutFeatureKey]: aboutReducer,
    [currentUserFeatureKey]: currentUserReducer
};
