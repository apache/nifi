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
import { concatLatestFrom } from '@ngrx/operators';
import * as EmptyQueueActions from './empty-queue.actions';
import * as ErrorActions from '../error/error.actions';
import { Store } from '@ngrx/store';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import {
    selectDropConnectionId,
    selectDropProcessGroupId,
    selectDropRequestEntity,
    selectDropSource
} from './empty-queue.selectors';
import { EmptyQueueService } from '../../service/empty-queue.service';
import { DropRequest } from './index';
import { CancelDialog } from '../../ui/common/cancel-dialog/cancel-dialog.component';
import { MatDialog } from '@angular/material/dialog';
import { isDefinedAndNotNull, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { OkDialog } from '../../ui/common/ok-dialog/ok-dialog.component';
import { resetEmptyQueueState } from './empty-queue.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../service/error-helper.service';

@Injectable()
export class EmptyQueueEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private emptyQueueService = inject(EmptyQueueService);
    private dialog = inject(MatDialog);
    private errorHelper = inject(ErrorHelper);

    promptEmptyQueueRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(EmptyQueueActions.promptEmptyQueueRequest),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Empty Queue',
                            message:
                                'Are you sure you want to empty this queue? All FlowFiles waiting at the time of the request will be removed.'
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            EmptyQueueActions.submitEmptyQueueRequest({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    submitEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.submitEmptyQueueRequest),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Empty Queue',
                        message: 'Waiting for queue to empty...'
                    },
                    disableClose: true
                });

                dialogReference.componentInstance.exit.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(EmptyQueueActions.stopPollingEmptyQueueRequest());
                });

                return from(this.emptyQueueService.submitEmptyQueueRequest(request)).pipe(
                    map((response) =>
                        EmptyQueueActions.submitEmptyQueueRequestSuccess({
                            response: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            EmptyQueueActions.emptyQueueApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                );
            })
        )
    );

    promptEmptyQueuesRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(EmptyQueueActions.promptEmptyQueuesRequest),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Empty All Queues',
                            message:
                                'Are you sure you want to empty all queues in this Process Group? All FlowFiles from all connections waiting at the time of the request will be removed.'
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            EmptyQueueActions.submitEmptyQueuesRequest({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    submitEmptyQueuesRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.submitEmptyQueuesRequest),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Empty All Queues',
                        message: 'Waiting for all queues to empty...'
                    },
                    disableClose: true
                });

                dialogReference.componentInstance.exit.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(EmptyQueueActions.stopPollingEmptyQueueRequest());
                });

                return from(this.emptyQueueService.submitEmptyQueuesRequest(request)).pipe(
                    map((response) =>
                        EmptyQueueActions.submitEmptyQueuesRequestSuccess({
                            response: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            EmptyQueueActions.emptyQueueApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                );
            })
        )
    );

    submitEmptyQueueRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.submitEmptyQueueRequestSuccess, EmptyQueueActions.submitEmptyQueuesRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const dropRequest: DropRequest = response.dropEntity.dropRequest;
                if (dropRequest.finished) {
                    return of(EmptyQueueActions.deleteEmptyQueueRequest());
                } else {
                    return of(EmptyQueueActions.startPollingEmptyQueueRequest());
                }
            })
        )
    );

    startPollingEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.startPollingEmptyQueueRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(EmptyQueueActions.stopPollingEmptyQueueRequest)))
                )
            ),
            switchMap(() => of(EmptyQueueActions.pollEmptyQueueRequest()))
        )
    );

    pollEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.pollEmptyQueueRequest),
            concatLatestFrom(() => [
                this.store.select(selectDropRequestEntity).pipe(isDefinedAndNotNull()),
                this.store.select(selectDropConnectionId),
                this.store.select(selectDropProcessGroupId)
            ]),
            switchMap(([, dropEntity, connectionId, processGroupId]) => {
                let poll$;
                if (connectionId) {
                    poll$ = this.emptyQueueService.pollEmptyQueueRequest({
                        connectionId,
                        dropRequestId: dropEntity.dropRequest.id
                    });
                } else if (processGroupId) {
                    poll$ = this.emptyQueueService.pollEmptyQueuesRequest({
                        processGroupId,
                        dropRequestId: dropEntity.dropRequest.id
                    });
                } else {
                    // The state machine should keep one of the identifiers populated for the
                    // lifetime of an in-flight drop request; if both are missing we cannot
                    // continue polling and surface an error rather than crashing on a non-null
                    // assertion.
                    return of(
                        EmptyQueueActions.emptyQueueApiError({
                            error: 'Unable to poll empty queue request: no connection or process group is associated with the active request.'
                        })
                    );
                }

                return from(poll$).pipe(
                    map((response) =>
                        EmptyQueueActions.pollEmptyQueueRequestSuccess({
                            response: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            EmptyQueueActions.emptyQueueApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                );
            })
        )
    );

    pollEmptyQueueRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.pollEmptyQueueRequestSuccess),
            map((action) => action.response),
            filter((response) => response.dropEntity.dropRequest.finished),
            switchMap(() => of(EmptyQueueActions.stopPollingEmptyQueueRequest()))
        )
    );

    stopPollingEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.stopPollingEmptyQueueRequest),
            switchMap(() => of(EmptyQueueActions.deleteEmptyQueueRequest()))
        )
    );

    deleteEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.deleteEmptyQueueRequest),
            concatLatestFrom(() => [
                this.store.select(selectDropRequestEntity).pipe(isDefinedAndNotNull()),
                this.store.select(selectDropConnectionId),
                this.store.select(selectDropProcessGroupId)
            ]),
            switchMap(([, dropEntity, connectionId, processGroupId]) => {
                this.dialog.closeAll();

                let delete$;
                if (connectionId) {
                    delete$ = this.emptyQueueService.deleteEmptyQueueRequest({
                        connectionId,
                        dropRequestId: dropEntity.dropRequest.id
                    });
                } else if (processGroupId) {
                    delete$ = this.emptyQueueService.deleteEmptyQueuesRequest({
                        processGroupId,
                        dropRequestId: dropEntity.dropRequest.id
                    });
                } else {
                    return of(
                        EmptyQueueActions.emptyQueueApiError({
                            error: 'Unable to delete empty queue request: no connection or process group is associated with the active request.'
                        })
                    );
                }

                return from(delete$).pipe(
                    map((response) =>
                        EmptyQueueActions.showEmptyQueueResults({
                            request: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError(() =>
                        of(
                            EmptyQueueActions.showEmptyQueueResults({
                                request: {
                                    dropEntity
                                }
                            })
                        )
                    )
                );
            })
        )
    );

    showEmptyQueueResults$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(EmptyQueueActions.showEmptyQueueResults),
                map((action) => action.request),
                concatLatestFrom(() => [
                    this.store.select(selectDropConnectionId),
                    this.store.select(selectDropProcessGroupId),
                    this.store.select(selectDropSource)
                ]),
                tap(([request, connectionId, processGroupId, source]) => {
                    const dropRequest: DropRequest = request.dropEntity.dropRequest;
                    const droppedTokens: string[] = dropRequest.dropped.split(/ \/ /);

                    let message = `${droppedTokens[0]} FlowFiles (${droppedTokens[1]})`;

                    if (dropRequest.percentCompleted < 100) {
                        const originalTokens: string[] = dropRequest.original.split(/ \/ /);
                        message = `${message} out of ${originalTokens[0]} (${originalTokens[1]})`;
                    }

                    if (connectionId) {
                        message = `${message} were removed from the queue.`;
                    } else if (processGroupId) {
                        message = `${message} were removed from the queues.`;
                    }

                    if (dropRequest.failureReason) {
                        message = `${message} ${dropRequest.failureReason}`;
                    }

                    // Page-specific refresh is handled by listeners on queueEmptied (filtered by source).
                    if (source) {
                        this.store.dispatch(
                            EmptyQueueActions.queueEmptied({
                                connectionId,
                                processGroupId,
                                source
                            })
                        );
                    }

                    const dialogReference = this.dialog.open(OkDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Empty Queue',
                            message
                        }
                    });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(resetEmptyQueueState());
                    });
                })
            ),
        { dispatch: false }
    );

    emptyQueueApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(EmptyQueueActions.emptyQueueApiError),
            map((action) => action.error),
            tap(() => {
                this.dialog.closeAll();
                this.store.dispatch(EmptyQueueActions.stopPollingEmptyQueueRequest());
            }),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );
}
