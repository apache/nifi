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
import { BulkReplayStatusState } from './index';
import {
    clearAllJobs,
    clearFinishedJobs,
    clearSuccessfulJobs,
    loadConfigSuccess,
    loadJobItemsSuccess,
    loadJobSummarySuccess,
    loadJobs,
    loadJobsFailure,
    loadJobsSuccess,
    refreshJobs,
    submitBulkReplaySuccess,
    updateConfigSuccess
} from './bulk-replay-status.actions';

export const initialState: BulkReplayStatusState = {
    jobs: [],
    jobItems: {},
    loadedTimestamp: 'N/A',
    loading: false,
    status: 'pending',
    deletedJobIds: [],
    config: null
};

export const bulkReplayStatusReducer = createReducer(
    initialState,

    on(submitBulkReplaySuccess, (state, { job }) => ({
        ...state,
        jobs: [job, ...state.jobs],
        loadedTimestamp: new Date().toLocaleString()
    })),

    on(loadJobs, (state) => ({
        ...state,
        loading: true
    })),

    on(loadJobsFailure, (state) => ({
        ...state,
        loading: false
    })),

    on(refreshJobs, (state) => ({
        ...state,
        loadedTimestamp: new Date().toLocaleString()
    })),

    on(loadJobSummarySuccess, (state, { job }) => {
        // Update the matching entry in jobs[] with live data from the server.
        const updatedJobs = state.jobs.map((j) => (j.jobId === job.jobId ? job : j));
        return { ...state, jobs: updatedJobs };
    }),

    on(loadJobItemsSuccess, (state, { jobId, items }) => ({
        ...state,
        jobItems: { ...state.jobItems, [jobId]: items }
    })),

    on(loadJobsSuccess, (state, { serverJobs }) => {
        const pendingDelete = new Set(state.deletedJobIds);
        const serverJobIds = new Set(serverJobs.map((j) => j.jobId));

        // Keep server jobs not pending deletion.
        const merged = serverJobs.filter((j) => !pendingDelete.has(j.jobId));

        // Retain local-only jobs not yet acknowledged by the server (e.g. just submitted).
        for (const localJob of state.jobs) {
            if (!serverJobIds.has(localJob.jobId) && !pendingDelete.has(localJob.jobId)) {
                merged.push(localJob);
            }
        }

        // Once the server no longer returns a deleted job, it's safe to remove from pending-delete.
        const remainingDeletedIds = state.deletedJobIds.filter((id) => serverJobIds.has(id));

        // Remove item cache entries for jobs that no longer exist.
        const updatedJobItems = { ...state.jobItems };
        for (const jobId of Object.keys(updatedJobItems)) {
            if (!serverJobIds.has(jobId) && !state.jobs.find((j) => j.jobId === jobId)) {
                delete updatedJobItems[jobId];
            }
        }

        return {
            ...state,
            jobs: merged,
            jobItems: updatedJobItems,
            loadedTimestamp: new Date().toLocaleString(),
            loading: false,
            deletedJobIds: remainingDeletedIds
        };
    }),

    on(clearSuccessfulJobs, (state, { jobIds }) => {
        const clearSet = new Set(jobIds);
        const remaining = state.jobs.filter((j) => !clearSet.has(j.jobId));
        const items = Object.fromEntries(remaining.map((j) => [j.jobId, state.jobItems[j.jobId]]).filter(([, v]) => v));
        return { ...state, jobs: remaining, jobItems: items, deletedJobIds: [...state.deletedJobIds, ...jobIds] };
    }),

    on(clearFinishedJobs, (state, { jobIds }) => {
        const clearSet = new Set(jobIds);
        const remaining = state.jobs.filter((j) => !clearSet.has(j.jobId));
        const items = Object.fromEntries(remaining.map((j) => [j.jobId, state.jobItems[j.jobId]]).filter(([, v]) => v));
        return { ...state, jobs: remaining, jobItems: items, deletedJobIds: [...state.deletedJobIds, ...jobIds] };
    }),

    on(clearAllJobs, (state) => ({
        ...state,
        jobs: [],
        jobItems: {},
        deletedJobIds: [...state.deletedJobIds, ...state.jobs.map((j) => j.jobId)]
    })),

    on(loadConfigSuccess, updateConfigSuccess, (state, { config }) => ({
        ...state,
        config
    }))
);
