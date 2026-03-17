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

import { createFeatureSelector } from '@ngrx/store';

export const bulkReplayStatusFeatureKey = 'bulkReplayStatus';

export type BulkReplayJobStatus =
    | 'QUEUED'
    | 'RUNNING'
    | 'COMPLETED'
    | 'PARTIAL_SUCCESS'
    | 'FAILED'
    | 'CANCELLED'
    | 'INTERRUPTED';

export type BulkReplayItemStatus = 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'SKIPPED';

/** Per-item status as returned by GET /bulk-replay/jobs/{id}/items */
export interface BulkReplayJobItem {
    itemId: string;
    itemIndex: number;
    provenanceEventId: number;
    clusterNodeId?: string;
    flowFileUuid: string;
    eventType: string;
    eventTime: string;
    componentName: string;
    status: BulkReplayItemStatus;
    errorMessage?: string;
    fileSizeBytes?: number;
    startTime?: string;
    endTime?: string;
    lastUpdated?: string;
}

/** Job summary as returned by GET /bulk-replay/jobs and GET /bulk-replay/jobs/{id} */
export interface BulkReplayJobSummary {
    jobId: string;
    jobName: string;
    processorId: string;
    processorName: string;
    processorType: string;
    groupId: string;
    submittedBy: string;
    submissionTime: string;
    startTime?: string;
    endTime?: string;
    lastUpdated?: string;
    status: BulkReplayJobStatus;
    statusMessage?: string;
    totalItems: number;
    processedItems: number;
    succeededItems: number;
    failedItems: number;
    percentComplete: number;
    /** ISO-8601 deadline for the current disconnect wait, or null when not waiting. */
    disconnectWaitDeadline?: string;
}

/** Submission body — extends summary with items list */
export interface BulkReplayJobDetail extends BulkReplayJobSummary {
    items: BulkReplayJobItem[];
}

export interface BulkReplayConfig {
    nodeDisconnectTimeout: string;
}

export interface BulkReplayStatusState {
    jobs: BulkReplayJobSummary[];
    /** Item-level state keyed by jobId; fetched on demand from GET /bulk-replay/jobs/{id}/items */
    jobItems: { [jobId: string]: BulkReplayJobItem[] };
    loadedTimestamp: string;
    loading: boolean;
    status: 'pending' | 'loading' | 'success' | 'error';
    /** IDs of jobs cleared locally; prevents loadJobsSuccess re-adding them before server DELETE completes. */
    deletedJobIds: string[];
    /** Runtime-tunable bulk replay configuration */
    config: BulkReplayConfig | null;
}

export const selectBulkReplayStatusState = createFeatureSelector<BulkReplayStatusState>(bulkReplayStatusFeatureKey);
