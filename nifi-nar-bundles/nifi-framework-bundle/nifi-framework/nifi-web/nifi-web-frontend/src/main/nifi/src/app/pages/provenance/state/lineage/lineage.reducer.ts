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
import { LineageState } from './index';
import {
    deleteLineageQuerySuccess,
    lineageApiError,
    pollLineageQuerySuccess,
    resetLineage,
    submitLineageQuery,
    submitLineageQuerySuccess
} from './lineage.actions';
import { produce } from 'immer';

export const initialState: LineageState = {
    activeLineage: null,
    completedLineage: {
        id: '',
        uri: '',
        submissionTime: '',
        expiration: '',
        percentCompleted: 0,
        finished: false,
        request: {
            lineageRequestType: 'FLOWFILE'
        },
        results: {
            nodes: [],
            links: []
        }
    },
    status: 'pending'
};

export const lineageReducer = createReducer(
    initialState,
    on(resetLineage, () => ({
        ...initialState
    })),
    on(submitLineageQuery, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(submitLineageQuerySuccess, pollLineageQuerySuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const lineage = response.lineage;
            draftState.activeLineage = lineage;

            // if the query has finished save it as completed, the active query will be reset after deletion
            if (lineage.finished) {
                draftState.completedLineage = lineage;
                draftState.status = 'success' as const;
            }
        });
    }),
    on(deleteLineageQuerySuccess, (state) => ({
        ...state,
        activeLineage: null
    })),
    on(lineageApiError, (state) => ({
        ...state,
        status: 'error' as const
    }))
);
