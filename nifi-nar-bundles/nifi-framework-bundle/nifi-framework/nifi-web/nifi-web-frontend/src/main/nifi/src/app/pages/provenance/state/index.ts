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

/*
  Provenance
 */

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { provenanceEventListingFeatureKey, ProvenanceEventListingState } from './provenance-event-listing';
import { provenanceEventListingReducer } from './provenance-event-listing/provenance-event-listing.reducer';
import { lineageFeatureKey, LineageState } from './lineage';
import { lineageReducer } from './lineage/lineage.reducer';

export const provenanceFeatureKey = 'provenance';

export interface ProvenanceState {
    [provenanceEventListingFeatureKey]: ProvenanceEventListingState;
    [lineageFeatureKey]: LineageState;
}

export function reducers(state: ProvenanceState | undefined, action: Action) {
    return combineReducers({
        [provenanceEventListingFeatureKey]: provenanceEventListingReducer,
        [lineageFeatureKey]: lineageReducer
    })(state, action);
}

export const selectProvenanceState = createFeatureSelector<ProvenanceState>(provenanceFeatureKey);
