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

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import * as QueueActions from './queue.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { Store } from '@ngrx/store';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { selectDropConnectionId, selectDropProcessGroupId, selectDropRequestEntity } from './queue.selectors';
import { QueueService } from '../../service/queue.service';
import { DropRequest } from './index';
import { CancelDialog } from '../../../../ui/common/cancel-dialog/cancel-dialog.component';
import { MatDialog } from '@angular/material/dialog';
import { isDefinedAndNotNull, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { loadChildProcessGroup, loadConnection, loadProcessGroup } from '../flow/flow.actions';
import { resetQueueState } from './queue.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { selectCurrentProcessGroupId } from '../flow/flow.selectors';

@Injectable()
export class QueueEffects {
    constructor(
        private actions$: Actions,
        private store: Store<CanvasState>,
        private queueService: QueueService,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper
    ) {}

    promptEmptyQueueRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(QueueActions.promptEmptyQueueRequest),
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
                            QueueActions.submitEmptyQueueRequest({
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
            ofType(QueueActions.submitEmptyQueueRequest),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Empty Queue',
                        message: 'Waiting for queue to empty...'
                    },
                    disableClose: true
                });

                dialogReference.componentInstance.close.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(QueueActions.stopPollingEmptyQueueRequest());
                });

                return from(this.queueService.submitEmptyQueueRequest(request)).pipe(
                    map((response) =>
                        QueueActions.submitEmptyQueueRequestSuccess({
                            response: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            QueueActions.queueApiError({
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
                ofType(QueueActions.promptEmptyQueuesRequest),
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
                            QueueActions.submitEmptyQueuesRequest({
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
            ofType(QueueActions.submitEmptyQueuesRequest),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Empty All Queues',
                        message: 'Waiting for all queues to empty...'
                    },
                    disableClose: true
                });

                dialogReference.componentInstance.close.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(QueueActions.stopPollingEmptyQueueRequest());
                });

                return from(this.queueService.submitEmptyQueuesRequest(request)).pipe(
                    map((response) =>
                        QueueActions.submitEmptyQueueRequestSuccess({
                            response: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            QueueActions.queueApiError({
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
            ofType(QueueActions.submitEmptyQueueRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const dropRequest: DropRequest = response.dropEntity.dropRequest;
                if (dropRequest.finished) {
                    return of(QueueActions.deleteEmptyQueueRequest());
                } else {
                    return of(QueueActions.startPollingEmptyQueueRequest());
                }
            })
        )
    );

    startPollingEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueActions.startPollingEmptyQueueRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(QueueActions.stopPollingEmptyQueueRequest)))
                )
            ),
            switchMap(() => of(QueueActions.pollEmptyQueueRequest()))
        )
    );

    pollEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueActions.pollEmptyQueueRequest),
            concatLatestFrom(() => this.store.select(selectDropRequestEntity).pipe(isDefinedAndNotNull())),
            switchMap(([, dropEntity]) => {
                return from(this.queueService.pollEmptyQueueRequest(dropEntity.dropRequest)).pipe(
                    map((response) =>
                        QueueActions.pollEmptyQueueRequestSuccess({
                            response: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            QueueActions.queueApiError({
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
            ofType(QueueActions.pollEmptyQueueRequestSuccess),
            map((action) => action.response),
            filter((response) => response.dropEntity.dropRequest.finished),
            switchMap(() => of(QueueActions.stopPollingEmptyQueueRequest()))
        )
    );

    stopPollingEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueActions.stopPollingEmptyQueueRequest),
            switchMap(() => of(QueueActions.deleteEmptyQueueRequest()))
        )
    );

    deleteEmptyQueueRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueActions.deleteEmptyQueueRequest),
            concatLatestFrom(() => this.store.select(selectDropRequestEntity).pipe(isDefinedAndNotNull())),
            switchMap(([, dropEntity]) => {
                this.dialog.closeAll();

                return from(this.queueService.deleteEmptyQueueRequest(dropEntity.dropRequest)).pipe(
                    map((response) =>
                        QueueActions.showEmptyQueueResults({
                            request: {
                                dropEntity: response
                            }
                        })
                    ),
                    catchError(() =>
                        of(
                            QueueActions.showEmptyQueueResults({
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
                ofType(QueueActions.showEmptyQueueResults),
                map((action) => action.request),
                concatLatestFrom(() => [
                    this.store.select(selectDropConnectionId),
                    this.store.select(selectDropProcessGroupId),
                    this.store.select(selectCurrentProcessGroupId)
                ]),
                tap(([request, connectionId, processGroupId, currentProcessGroupId]) => {
                    const dropRequest: DropRequest = request.dropEntity.dropRequest;
                    const droppedTokens: string[] = dropRequest.dropped.split(/ \/ /);

                    let message = `${droppedTokens[0]} FlowFiles (${droppedTokens[1]})`;

                    if (dropRequest.percentCompleted < 100) {
                        const originalTokens: string[] = dropRequest.original.split(/ \/ /);
                        message = `${message} out of ${originalTokens[0]} (${originalTokens[1]})`;
                    }

                    if (connectionId) {
                        message = `${message} were removed from the queue.`;

                        this.store.dispatch(
                            loadConnection({
                                id: connectionId
                            })
                        );
                    } else if (processGroupId) {
                        message = `${message} were removed from the queues.`;

                        if (processGroupId === currentProcessGroupId) {
                            this.store.dispatch(
                                loadProcessGroup({
                                    request: {
                                        id: processGroupId,
                                        transitionRequired: false
                                    }
                                })
                            );
                        } else {
                            this.store.dispatch(
                                loadChildProcessGroup({
                                    request: {
                                        id: processGroupId
                                    }
                                })
                            );
                        }
                    }

                    if (dropRequest.failureReason) {
                        message = `${message} ${dropRequest.failureReason}`;
                    }

                    const dialogReference = this.dialog.open(OkDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Empty Queue',
                            message
                        }
                    });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(resetQueueState());
                    });
                })
            ),
        { dispatch: false }
    );

    queueApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueActions.queueApiError),
            map((action) => action.error),
            tap(() => {
                this.dialog.closeAll();
                this.store.dispatch(QueueActions.stopPollingEmptyQueueRequest());
            }),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );
}
