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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { from, interval, of, EMPTY } from 'rxjs';
import { catchError, exhaustMap, map, mergeMap, startWith, switchMap, takeWhile, tap } from 'rxjs/operators';
import { MatSnackBar } from '@angular/material/snack-bar';
import { BulkReplayService } from '../service/bulk-replay.service';
import { BulkReplayJobDetail, BulkReplayJobItem, BulkReplayJobStatus, BulkReplayJobSummary } from './index';
import { ProvenanceEventSummary } from '../../../state/shared';
import * as BulkReplayStatusActions from './bulk-replay-status.actions';

const TERMINAL_STATUSES = new Set<BulkReplayJobStatus>([
    'COMPLETED',
    'PARTIAL_SUCCESS',
    'FAILED',
    'CANCELLED',
    'INTERRUPTED'
]);

@Injectable()
export class BulkReplayStatusEffects {
    private actions$ = inject(Actions);
    private bulkReplayService = inject(BulkReplayService);
    private snackBar = inject(MatSnackBar);

    /**
     * Build the submission payload from the selected provenance events and POST to the server.
     * The server is immediately authoritative — no local state merging needed.
     */
    submitBulkReplay$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.submitBulkReplay),
            exhaustMap(({ events, processorId, processorName, processorType, groupId, jobName }) => {
                const items: BulkReplayJobItem[] = events.map((e: ProvenanceEventSummary, index: number) => ({
                    itemId: '', // assigned by server
                    itemIndex: index,
                    provenanceEventId: e.eventId,
                    clusterNodeId: e.clusterNodeId,
                    flowFileUuid: e.flowFileUuid,
                    eventType: e.eventType,
                    eventTime: e.eventTime,
                    componentName: e.componentName,
                    fileSizeBytes: e.fileSizeBytes,
                    status: 'QUEUED'
                }));

                const now = new Date();
                const defaultJobName = `${processorName} ${now.toISOString().replace(/\.\d{3}Z$/, 'Z')}`;
                const resolvedJobName = jobName?.trim() || defaultJobName;

                const detail: BulkReplayJobDetail = {
                    jobId: '', // assigned by server
                    jobName: resolvedJobName,
                    processorId,
                    processorName,
                    processorType,
                    groupId,
                    submittedBy: '', // assigned by server
                    submissionTime: now.toISOString(),
                    status: 'QUEUED',
                    totalItems: events.length,
                    processedItems: 0,
                    succeededItems: 0,
                    failedItems: 0,
                    percentComplete: 0,
                    items
                };

                return this.bulkReplayService.submitJob(detail).pipe(
                    map((job: BulkReplayJobSummary) => BulkReplayStatusActions.submitBulkReplaySuccess({ job })),
                    catchError((err) =>
                        of(
                            BulkReplayStatusActions.submitBulkReplayFailure({
                                error: err?.error?.message || err?.message || 'Failed to submit job'
                            })
                        )
                    )
                );
            })
        )
    );

    /**
     * After a job is submitted, poll GET /bulk-replay/jobs/{id} every 2 s until terminal.
     * The summary drives the table; items are fetched separately when the detail dialog is open.
     */
    pollJobSummary$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.submitBulkReplaySuccess),
            mergeMap(({ job }) =>
                interval(2000).pipe(
                    startWith(0),
                    switchMap(() =>
                        this.bulkReplayService
                            .getJobSummary(job.jobId)
                            .pipe(catchError((err) => (err.status === 404 ? of(null) : of(job))))
                    ),
                    // Stop polling when the job reaches a terminal status or is deleted (null from 404).
                    takeWhile((summary) => summary !== null && !TERMINAL_STATUSES.has(summary.status), true),
                    map((summary) => {
                        if (summary === null) {
                            return BulkReplayStatusActions.loadJobSummarySuccess({
                                job: { ...job, status: 'COMPLETED' as const }
                            });
                        }
                        return BulkReplayStatusActions.loadJobSummarySuccess({ job: summary });
                    })
                )
            )
        )
    );

    /** Show a snackbar notification when a job is successfully submitted. */
    submitBulkReplaySuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BulkReplayStatusActions.submitBulkReplaySuccess),
                tap(({ job }) => {
                    this.snackBar.open(`Bulk replay job "${job.jobName}" created.`, 'Dismiss', { duration: 8000 });
                })
            ),
        { dispatch: false }
    );

    /** Fetch updated summary for a single job on demand. */
    loadJobSummary$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.loadJobSummary),
            mergeMap(({ jobId }) =>
                this.bulkReplayService.getJobSummary(jobId).pipe(
                    map((job) => BulkReplayStatusActions.loadJobSummarySuccess({ job })),
                    catchError(() => EMPTY)
                )
            )
        )
    );

    /** Fetch item-level status for a job (called when opening the detail dialog). */
    loadJobItems$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.loadJobItems),
            mergeMap(({ jobId }) =>
                this.bulkReplayService.getJobItems(jobId).pipe(
                    map((items) => BulkReplayStatusActions.loadJobItemsSuccess({ jobId, items })),
                    catchError(() => EMPTY)
                )
            )
        )
    );

    /** Load all server-side jobs on page init or manual refresh. */
    loadJobs$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.loadJobs),
            mergeMap(() =>
                this.bulkReplayService.getJobs().pipe(
                    map((serverJobs) => BulkReplayStatusActions.loadJobsSuccess({ serverJobs })),
                    catchError((err) =>
                        of(
                            BulkReplayStatusActions.loadJobsFailure({
                                error: err?.message ?? 'Failed to load jobs'
                            })
                        )
                    )
                )
            )
        )
    );

    /** Cancel a running job. */
    cancelJob$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BulkReplayStatusActions.cancelJob),
                mergeMap(({ jobId }) =>
                    this.bulkReplayService.cancelJob(jobId).pipe(
                        mergeMap(() => EMPTY),
                        catchError((err) => {
                            this.snackBar.open(
                                err?.error?.message || err?.message || 'Failed to cancel job',
                                'Dismiss',
                                { duration: 8000 }
                            );
                            return EMPTY;
                        })
                    )
                )
            ),
        { dispatch: false }
    );

    /** Delete a job from the server when it is cleared locally. */
    deleteJobOnServer$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BulkReplayStatusActions.deleteJobOnServer),
                mergeMap(({ jobId }) =>
                    this.bulkReplayService.deleteJob(jobId).pipe(
                        mergeMap(() => EMPTY),
                        catchError((err) => {
                            this.snackBar.open(
                                err?.error?.message || err?.message || 'Failed to delete job',
                                'Dismiss',
                                { duration: 8000 }
                            );
                            return EMPTY;
                        })
                    )
                )
            ),
        { dispatch: false }
    );

    clearJobs$ = createEffect(() =>
        this.actions$.pipe(
            ofType(
                BulkReplayStatusActions.clearSuccessfulJobs,
                BulkReplayStatusActions.clearFinishedJobs,
                BulkReplayStatusActions.clearAllJobs
            ),
            mergeMap(({ jobIds }) => from(jobIds.map((jobId) => BulkReplayStatusActions.deleteJobOnServer({ jobId }))))
        )
    );

    /** Show snackbar on load jobs failure. */
    loadJobsFailure$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BulkReplayStatusActions.loadJobsFailure),
                tap(({ error }) => {
                    this.snackBar.open(error, 'Dismiss', { duration: 8000 });
                })
            ),
        { dispatch: false }
    );

    /** Load bulk replay configuration from the server. */
    loadConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.loadConfig),
            mergeMap(() =>
                this.bulkReplayService.getConfig().pipe(
                    map((config) => BulkReplayStatusActions.loadConfigSuccess({ config })),
                    catchError(() => EMPTY)
                )
            )
        )
    );

    /** Update bulk replay configuration on the server. */
    updateConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulkReplayStatusActions.updateConfig),
            mergeMap(({ config }) =>
                this.bulkReplayService.updateConfig(config).pipe(
                    map((updated) => BulkReplayStatusActions.updateConfigSuccess({ config: updated })),
                    catchError((err) =>
                        of(
                            BulkReplayStatusActions.updateConfigFailure({
                                error: err?.error?.message || err?.message || 'Failed to update config'
                            })
                        )
                    )
                )
            )
        )
    );

    /** Show snackbar on config update success. */
    updateConfigSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BulkReplayStatusActions.updateConfigSuccess),
                tap(({ config }) => {
                    this.snackBar.open(
                        `Node disconnect timeout updated to ${config.nodeDisconnectTimeout}`,
                        'Dismiss',
                        { duration: 5000 }
                    );
                })
            ),
        { dispatch: false }
    );

    /** Show snackbar on config update failure. */
    updateConfigFailure$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(BulkReplayStatusActions.updateConfigFailure),
                tap(({ error }) => {
                    this.snackBar.open(error, 'Dismiss', { duration: 8000 });
                })
            ),
        { dispatch: false }
    );
}
