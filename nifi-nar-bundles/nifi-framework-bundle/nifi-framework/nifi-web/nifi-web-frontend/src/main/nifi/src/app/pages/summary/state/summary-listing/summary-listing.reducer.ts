/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { createReducer, on } from '@ngrx/store';
import { ProcessGroupStatusSnapshot, ProcessorStatusSnapshotEntity, SummaryListingState } from './index';
import {
    loadSummaryListing,
    loadSummaryListingSuccess,
    resetSummaryState,
    summaryListingApiError
} from './summary-listing.actions';

export const initialState: SummaryListingState = {
    clusterSummary: null,
    processGroupStatus: null,
    processorStatusSnapshots: [],
    status: 'pending',
    error: null,
    loadedTimestamp: ''
};

export const summaryListingReducer = createReducer(
    initialState,

    on(loadSummaryListing, (state) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadSummaryListingSuccess, (state, { response }) => {
        const processors: ProcessorStatusSnapshotEntity[] = flattenProcessorStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );

        return {
            ...state,
            error: null,
            status: 'success' as const,
            loadedTimestamp: response.status.processGroupStatus.statsLastRefreshed,
            processGroupStatus: response.status,
            clusterSummary: response.clusterSummary,
            processorStatusSnapshots: processors
        };
    }),

    on(summaryListingApiError, (state, { error }) => ({
        ...state,
        error,
        status: 'error' as const
    })),

    on(resetSummaryState, (state) => ({
        ...initialState
    }))
);

function flattenProcessorStatusSnapshots(
    snapshot: ProcessGroupStatusSnapshot,
    parentPath: string = ''
): ProcessorStatusSnapshotEntity[] {
    const path: string = `${parentPath}/${snapshot.name}`;
    // supplement the processors with the parent process group name
    const processors = snapshot.processorStatusSnapshots.map((p) => {
        return {
            ...p,
            processorStatusSnapshot: {
                ...p.processorStatusSnapshot,
                parentProcessGroupName: snapshot.name,
                processGroupNamePath: path
            }
        };
    });

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenProcessorStatusSnapshots(pg, path));
        return [...processors, ...children];
    } else {
        return processors;
    }
}
