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

import { createAction, props } from '@ngrx/store';
import { BulkReplayConfig, BulkReplayJobItem, BulkReplayJobSummary } from './index';
import { ProvenanceEventSummary } from '../../../state/shared';

export const submitBulkReplay = createAction(
    '[Bulk Replay Status] Submit Bulk Replay',
    props<{
        events: ProvenanceEventSummary[];
        processorId: string;
        processorName: string;
        processorType: string;
        groupId: string;
        jobName?: string;
    }>()
);

export const submitBulkReplaySuccess = createAction(
    '[Bulk Replay Status] Submit Bulk Replay Success',
    props<{ job: BulkReplayJobSummary }>()
);

export const submitBulkReplayFailure = createAction(
    '[Bulk Replay Status] Submit Bulk Replay Failure',
    props<{ error: string }>()
);

// Load all server-side jobs (called on page init and refresh)
export const loadJobs = createAction('[Bulk Replay Status] Load Jobs');
export const loadJobsSuccess = createAction(
    '[Bulk Replay Status] Load Jobs Success',
    props<{ serverJobs: BulkReplayJobSummary[] }>()
);
export const loadJobsFailure = createAction('[Bulk Replay Status] Load Jobs Failure', props<{ error: string }>());

// Refresh the summary for a single job from GET /bulk-replay/jobs/{id}
export const loadJobSummary = createAction('[Bulk Replay Status] Load Job Summary', props<{ jobId: string }>());
export const loadJobSummarySuccess = createAction(
    '[Bulk Replay Status] Load Job Summary Success',
    props<{ job: BulkReplayJobSummary }>()
);

// Fetch item-level status from GET /bulk-replay/jobs/{id}/items
export const loadJobItems = createAction('[Bulk Replay Status] Load Job Items', props<{ jobId: string }>());
export const loadJobItemsSuccess = createAction(
    '[Bulk Replay Status] Load Job Items Success',
    props<{ jobId: string; items: BulkReplayJobItem[] }>()
);

export const refreshJobs = createAction('[Bulk Replay Status] Refresh Jobs');

export const clearSuccessfulJobs = createAction(
    '[Bulk Replay Status] Clear Successful Jobs',
    props<{ jobIds: string[] }>()
);
export const clearFinishedJobs = createAction(
    '[Bulk Replay Status] Clear Finished Jobs',
    props<{ jobIds: string[] }>()
);
export const clearAllJobs = createAction('[Bulk Replay Status] Clear All Jobs', props<{ jobIds: string[] }>());

// Request cancellation of a running job
export const cancelJob = createAction('[Bulk Replay Status] Cancel Job', props<{ jobId: string }>());

// Delete a job from the server (triggered by clear actions)
export const deleteJobOnServer = createAction('[Bulk Replay Status] Delete Job On Server', props<{ jobId: string }>());

// Configuration
export const loadConfig = createAction('[Bulk Replay Status] Load Config');
export const loadConfigSuccess = createAction(
    '[Bulk Replay Status] Load Config Success',
    props<{ config: BulkReplayConfig }>()
);
export const updateConfig = createAction('[Bulk Replay Status] Update Config', props<{ config: BulkReplayConfig }>());
export const updateConfigSuccess = createAction(
    '[Bulk Replay Status] Update Config Success',
    props<{ config: BulkReplayConfig }>()
);
export const updateConfigFailure = createAction(
    '[Bulk Replay Status] Update Config Failure',
    props<{ error: string }>()
);
