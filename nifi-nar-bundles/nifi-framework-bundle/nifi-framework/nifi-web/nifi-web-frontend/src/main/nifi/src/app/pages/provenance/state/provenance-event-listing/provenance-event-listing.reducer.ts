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
import { ProvenanceEventListingState } from './index';
import {
    clearProvenanceRequest,
    deleteProvenanceQuerySuccess,
    loadProvenanceOptionsSuccess,
    pollProvenanceQuerySuccess,
    provenanceApiError,
    resetProvenanceState,
    saveProvenanceRequest,
    submitProvenanceQuery,
    submitProvenanceQuerySuccess
} from './provenance-event-listing.actions';
import { produce } from 'immer';

export const initialState: ProvenanceEventListingState = {
    options: null,
    request: null,
    activeProvenance: null,
    completedProvenance: {
        id: '',
        uri: '',
        submissionTime: '',
        expiration: '',
        percentCompleted: 0,
        finished: false,
        request: {
            maxResults: 0,
            summarize: true,
            incrementalResults: false
        },
        results: {
            provenanceEvents: [],
            total: '',
            totalCount: 0,
            generated: 'N/A',
            oldestEvent: 'N/A',
            timeOffset: 0,
            errors: []
        }
    },
    loadedTimestamp: 'N/A',
    status: 'pending'
};

export const provenanceEventListingReducer = createReducer(
    initialState,
    on(resetProvenanceState, () => ({
        ...initialState
    })),
    on(loadProvenanceOptionsSuccess, (state, { response }) => ({
        ...state,
        options: response.provenanceOptions
    })),
    on(submitProvenanceQuery, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(submitProvenanceQuerySuccess, pollProvenanceQuerySuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const provenance = response.provenance;
            draftState.activeProvenance = provenance;

            // if the query has finished save it as completed, the active query will be reset after deletion
            if (provenance.finished) {
                draftState.completedProvenance = provenance;
                draftState.loadedTimestamp = provenance.results.generated;
                draftState.status = 'success' as const;
            }
        });
    }),
    on(deleteProvenanceQuerySuccess, (state) => ({
        ...state,
        activeProvenance: null
    })),
    on(saveProvenanceRequest, (state, { request }) => ({
        ...state,
        request
    })),
    on(clearProvenanceRequest, (state) => ({
        ...state,
        request: null
    })),
    on(provenanceApiError, (state) => ({
        ...state,
        status: 'error' as const
    }))
);
