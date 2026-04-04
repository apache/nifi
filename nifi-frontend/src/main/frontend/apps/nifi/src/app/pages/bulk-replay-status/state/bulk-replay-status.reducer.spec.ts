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

import { bulkReplayStatusReducer, initialState } from './bulk-replay-status.reducer';
import * as BulkReplayStatusActions from './bulk-replay-status.actions';
import { BulkReplayJobItem, BulkReplayJobSummary, BulkReplayStatusState } from './index';

describe('BulkReplay Status Reducer', () => {
    function createJob(jobId: string, overrides: Partial<BulkReplayJobSummary> = {}): BulkReplayJobSummary {
        return {
            jobId,
            jobName: `Test Job ${jobId}`,
            submittedBy: 'admin',
            submissionTime: '2024-01-01T00:00:00.000Z',
            status: 'RUNNING',
            percentComplete: 0,
            totalItems: 3,
            processedItems: 0,
            succeededItems: 0,
            failedItems: 0,
            groupId: 'group-1',
            processorId: 'proc-1',
            processorName: 'TestProcessor',
            processorType: 'GenerateFlowFile',
            ...overrides
        };
    }

    function createItem(provenanceEventId: number, status: BulkReplayJobItem['status'] = 'QUEUED'): BulkReplayJobItem {
        return {
            itemId: `item-${provenanceEventId}`,
            itemIndex: provenanceEventId - 1,
            provenanceEventId,
            flowFileUuid: `uuid-${provenanceEventId}`,
            eventType: 'CREATE',
            eventTime: '01/01/2024 00:00:00 EST',
            componentName: 'TestProcessor',
            status
        };
    }

    it('should return the initial state', () => {
        const result = bulkReplayStatusReducer(undefined, {} as any);
        expect(result).toBe(initialState);
    });

    describe('submitBulkReplaySuccess', () => {
        it('should prepend the job to the jobs array', () => {
            const existingJob = createJob('job-0');
            const state: BulkReplayStatusState = { ...initialState, jobs: [existingJob] };
            const newJob = createJob('job-1');

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.submitBulkReplaySuccess({ job: newJob })
            );

            expect(result.jobs.length).toBe(2);
            expect(result.jobs[0].jobId).toBe('job-1');
            expect(result.jobs[1].jobId).toBe('job-0');
        });

        it('should update loadedTimestamp', () => {
            const job = createJob('job-1');
            const result = bulkReplayStatusReducer(
                initialState,
                BulkReplayStatusActions.submitBulkReplaySuccess({ job })
            );
            expect(result.loadedTimestamp).not.toBe('N/A');
        });
    });

    describe('loadJobSummarySuccess', () => {
        it('should update the matching job summary in jobs[]', () => {
            const job = createJob('job-1', { status: 'RUNNING', succeededItems: 0, percentComplete: 0 });
            const updatedJob = createJob('job-1', { status: 'COMPLETED', succeededItems: 3, percentComplete: 100 });
            const state: BulkReplayStatusState = { ...initialState, jobs: [job] };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobSummarySuccess({ job: updatedJob })
            );

            const found = result.jobs.find((j) => j.jobId === 'job-1');
            expect(found?.status).toBe('COMPLETED');
            expect(found?.succeededItems).toBe(3);
        });

        it('should not affect other jobs', () => {
            const job1 = createJob('job-1', { status: 'RUNNING' });
            const job2 = createJob('job-2', { status: 'QUEUED' });
            const updatedJob1 = createJob('job-1', { status: 'COMPLETED' });
            const state: BulkReplayStatusState = { ...initialState, jobs: [job1, job2] };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobSummarySuccess({ job: updatedJob1 })
            );

            expect(result.jobs.find((j) => j.jobId === 'job-2')?.status).toBe('QUEUED');
        });
    });

    describe('loadJobSummaryDeleted', () => {
        it('should remove the deleted job and its cached items', () => {
            const deletedJob = createJob('job-1', { status: 'RUNNING' });
            const otherJob = createJob('job-2', { status: 'QUEUED' });
            const state: BulkReplayStatusState = {
                ...initialState,
                jobs: [deletedJob, otherJob],
                jobItems: { 'job-1': [createItem(1)], 'job-2': [createItem(2)] }
            };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobSummaryDeleted({ jobId: 'job-1' })
            );

            expect(result.jobs.map((job) => job.jobId)).toEqual(['job-2']);
            expect(result.jobItems['job-1']).toBeUndefined();
            expect(result.jobItems['job-2']).toEqual([createItem(2)]);
        });
    });

    describe('loadJobItemsSuccess', () => {
        it('should store items keyed by jobId', () => {
            const items = [createItem(1), createItem(2)];
            const result = bulkReplayStatusReducer(
                initialState,
                BulkReplayStatusActions.loadJobItemsSuccess({ jobId: 'job-1', items })
            );

            expect(result.jobItems['job-1']).toEqual(items);
        });

        it('should not overwrite items for other jobs', () => {
            const existingItems = [createItem(99)];
            const state: BulkReplayStatusState = { ...initialState, jobItems: { 'job-2': existingItems } };
            const newItems = [createItem(1)];

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobItemsSuccess({ jobId: 'job-1', items: newItems })
            );

            expect(result.jobItems['job-2']).toEqual(existingItems);
            expect(result.jobItems['job-1']).toEqual(newItems);
        });
    });

    describe('clearSuccessfulJobs', () => {
        it('should remove only COMPLETED jobs', () => {
            const completed = createJob('job-1', { status: 'COMPLETED' });
            const partial = createJob('job-2', { status: 'PARTIAL_SUCCESS' });
            const running = createJob('job-3', { status: 'RUNNING' });
            const state: BulkReplayStatusState = {
                ...initialState,
                jobs: [completed, partial, running],
                jobItems: { 'job-1': [createItem(1)], 'job-2': [createItem(2)], 'job-3': [createItem(3)] }
            };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.clearSuccessfulJobs({ jobIds: ['job-1'] })
            );

            expect(result.jobs.map((j) => j.jobId)).toEqual(['job-2', 'job-3']);
            expect(result.jobItems['job-1']).toBeUndefined();
        });
    });

    describe('clearFinishedJobs', () => {
        it('should remove COMPLETED, PARTIAL_SUCCESS, FAILED, CANCELLED, and INTERRUPTED jobs', () => {
            const completed = createJob('job-1', { status: 'COMPLETED' });
            const partial = createJob('job-2', { status: 'PARTIAL_SUCCESS' });
            const failed = createJob('job-3', { status: 'FAILED' });
            const cancelled = createJob('job-4', { status: 'CANCELLED' });
            const running = createJob('job-5', { status: 'RUNNING' });
            const interrupted = createJob('job-6', { status: 'INTERRUPTED' });
            const state: BulkReplayStatusState = {
                ...initialState,
                jobs: [completed, partial, failed, cancelled, running, interrupted]
            };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.clearFinishedJobs({ jobIds: ['job-1', 'job-2', 'job-3', 'job-4', 'job-6'] })
            );

            expect(result.jobs.map((j) => j.jobId)).toEqual(['job-5']);
        });
    });

    describe('clearAllJobs', () => {
        it('should empty jobs and jobItems', () => {
            const job = createJob('job-1');
            const state: BulkReplayStatusState = {
                ...initialState,
                jobs: [job],
                jobItems: { 'job-1': [createItem(1)] }
            };

            const result = bulkReplayStatusReducer(state, BulkReplayStatusActions.clearAllJobs({ jobIds: ['job-1'] }));

            expect(result.jobs).toEqual([]);
            expect(result.jobItems).toEqual({});
        });
    });

    describe('refreshJobs', () => {
        it('should update loadedTimestamp without changing jobs', () => {
            const job = createJob('job-1');
            const state: BulkReplayStatusState = { ...initialState, jobs: [job], loadedTimestamp: 'N/A' };

            const result = bulkReplayStatusReducer(state, BulkReplayStatusActions.refreshJobs());

            expect(result.jobs).toBe(state.jobs);
            expect(result.loadedTimestamp).not.toBe('N/A');
        });
    });

    describe('loadJobs', () => {
        it('should set loading to true', () => {
            const result = bulkReplayStatusReducer(initialState, BulkReplayStatusActions.loadJobs());
            expect(result.loading).toBe(true);
        });
    });

    describe('loadJobsFailure', () => {
        it('should set loading to false', () => {
            const state: BulkReplayStatusState = { ...initialState, loading: true };
            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobsFailure({ error: 'Network error' })
            );
            expect(result.loading).toBe(false);
        });
    });

    describe('loadJobsSuccess', () => {
        it('should add server jobs', () => {
            const serverJob = createJob('server-job', { status: 'COMPLETED' });
            const result = bulkReplayStatusReducer(
                initialState,
                BulkReplayStatusActions.loadJobsSuccess({ serverJobs: [serverJob] })
            );

            expect(result.jobs.length).toBe(1);
            expect(result.jobs[0].jobId).toBe('server-job');
        });

        it('should keep local-only jobs not yet on the server', () => {
            const localJob = createJob('local-only', { status: 'RUNNING' });
            const serverJob = createJob('server-only', { status: 'COMPLETED' });
            const state: BulkReplayStatusState = { ...initialState, jobs: [localJob] };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobsSuccess({ serverJobs: [serverJob] })
            );

            expect(result.jobs.some((j) => j.jobId === 'local-only')).toBe(true);
            expect(result.jobs.some((j) => j.jobId === 'server-only')).toBe(true);
        });

        it('should exclude jobs pending deletion', () => {
            const serverJob = createJob('job-1', { status: 'COMPLETED' });
            const state: BulkReplayStatusState = { ...initialState, deletedJobIds: ['job-1'] };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobsSuccess({ serverJobs: [serverJob] })
            );

            expect(result.jobs.some((j) => j.jobId === 'job-1')).toBe(false);
        });

        it('should prefer server version when local and server jobs share the same id', () => {
            const localJob = createJob('job-1', { status: 'RUNNING', succeededItems: 0 });
            const serverJob = createJob('job-1', { status: 'COMPLETED', succeededItems: 3 });
            const state: BulkReplayStatusState = { ...initialState, jobs: [localJob] };

            const result = bulkReplayStatusReducer(
                state,
                BulkReplayStatusActions.loadJobsSuccess({ serverJobs: [serverJob] })
            );

            expect(result.jobs.length).toBe(1);
            expect(result.jobs[0].status).toBe('COMPLETED');
            expect(result.jobs[0].succeededItems).toBe(3);
        });

        it('should clean up deletedJobIds once server no longer returns the job', () => {
            const state: BulkReplayStatusState = { ...initialState, deletedJobIds: ['gone-job'] };

            const result = bulkReplayStatusReducer(state, BulkReplayStatusActions.loadJobsSuccess({ serverJobs: [] }));

            expect(result.deletedJobIds).toEqual([]);
        });

        it('should update loadedTimestamp', () => {
            const result = bulkReplayStatusReducer(
                initialState,
                BulkReplayStatusActions.loadJobsSuccess({ serverJobs: [] })
            );

            expect(result.loadedTimestamp).not.toBe('N/A');
        });
    });
});
