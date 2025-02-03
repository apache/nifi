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
import * as QueueListingActions from './queue-listing.actions';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../flow-designer/state';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { selectConnectionIdFromRoute, selectActiveListingRequest } from './queue-listing.selectors';
import { QueueService } from '../../service/queue.service';
import { ListingRequest } from './index';
import { CancelDialog } from '../../../../ui/common/cancel-dialog/cancel-dialog.component';
import { MatDialog } from '@angular/material/dialog';
import { selectAbout } from '../../../../state/about/about.selectors';
import { FlowFileDialog } from '../../ui/queue-listing/flowfile-dialog/flowfile-dialog.component';
import { isDefinedAndNotNull, NiFiCommon, LARGE_DIALOG } from '@nifi/shared';
import { HttpErrorResponse } from '@angular/common/http';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { stopPollingQueueListingRequest } from './queue-listing.actions';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class QueueListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<CanvasState>,
        private queueService: QueueService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private nifiCommon: NiFiCommon
    ) {}

    loadConnectionLabel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.loadConnectionLabel),
            map((action) => action.request),
            switchMap((request) =>
                from(this.queueService.getConnection(request.connectionId)).pipe(
                    map((response) => {
                        const connection: any = response.component;

                        let connectionLabel = 'Connection';
                        if (!this.nifiCommon.isBlank(connection.name)) {
                            connectionLabel = connection.name;
                        } else if (connection.selectedRelationships) {
                            connectionLabel = connection.selectedRelationships.join(', ');
                        }

                        return QueueListingActions.loadConnectionLabelSuccess({
                            response: {
                                connectionId: request.connectionId,
                                connectionLabel
                            }
                        });
                    }),
                    catchError(() =>
                        of(
                            QueueListingActions.loadConnectionLabelSuccess({
                                response: {
                                    connectionId: request.connectionId,
                                    connectionLabel: 'Connection'
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    submitQueueListingRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.submitQueueListingRequest),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Queue Listing',
                        message: 'Waiting for queue listing to complete...'
                    },
                    disableClose: true
                });

                dialogReference.componentInstance.close.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(QueueListingActions.stopPollingQueueListingRequest());
                });

                return from(this.queueService.submitQueueListingRequest(request)).pipe(
                    map((response) =>
                        QueueListingActions.submitQueueListingRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                QueueListingActions.queueListingApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.store.dispatch(stopPollingQueueListingRequest());

                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                );
            })
        )
    );

    resubmitQueueListingRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.resubmitQueueListingRequest),
            concatLatestFrom(() => this.store.select(selectConnectionIdFromRoute)),
            switchMap(([, connectionId]) =>
                of(QueueListingActions.submitQueueListingRequest({ request: { connectionId } }))
            )
        )
    );

    submitQueueListingRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.submitQueueListingRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const listingRequest: ListingRequest = response.requestEntity.listingRequest;
                if (listingRequest.finished) {
                    return of(QueueListingActions.deleteQueueListingRequest());
                } else {
                    return of(QueueListingActions.startPollingQueueListingRequest());
                }
            })
        )
    );

    startPollingQueueListingRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.startPollingQueueListingRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(QueueListingActions.stopPollingQueueListingRequest)))
                )
            ),
            switchMap(() => of(QueueListingActions.pollQueueListingRequest()))
        )
    );

    pollQueueListingRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.pollQueueListingRequest),
            concatLatestFrom(() => this.store.select(selectActiveListingRequest).pipe(isDefinedAndNotNull())),
            switchMap(([, listingRequest]) => {
                return from(this.queueService.pollQueueListingRequest(listingRequest)).pipe(
                    map((response) =>
                        QueueListingActions.pollQueueListingRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                QueueListingActions.queueListingApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.store.dispatch(stopPollingQueueListingRequest());

                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                );
            })
        )
    );

    pollQueueListingRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.pollQueueListingRequestSuccess),
            map((action) => action.response),
            filter((response) => response.requestEntity.listingRequest.finished),
            switchMap(() => of(QueueListingActions.stopPollingQueueListingRequest()))
        )
    );

    stopPollingQueueListingRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.stopPollingQueueListingRequest),
            switchMap(() => of(QueueListingActions.deleteQueueListingRequest()))
        )
    );

    deleteQueueListingRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.deleteQueueListingRequest),
            concatLatestFrom(() => this.store.select(selectActiveListingRequest)),
            tap(([, listingRequest]) => {
                this.dialog.closeAll();

                if (listingRequest) {
                    this.queueService.deleteQueueListingRequest(listingRequest).subscribe({
                        error: (errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                            );
                        }
                    });
                }
            }),
            switchMap(() => of(QueueListingActions.deleteQueueListingRequestSuccess()))
        )
    );

    viewFlowFile$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.viewFlowFile),
            map((action) => action.request),
            switchMap((request) =>
                from(this.queueService.getFlowFile(request.flowfileSummary)).pipe(
                    map((response) =>
                        QueueListingActions.openFlowFileDialog({
                            request: {
                                flowfile: response.flowFile,
                                clusterNodeId: request.flowfileSummary.clusterNodeId
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ErrorActions.snackBarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    openFlowFileDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(QueueListingActions.openFlowFileDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectAbout)),
                filter((about) => about != null),
                tap(([request, about]) => {
                    const dialogReference = this.dialog.open(FlowFileDialog, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    dialogReference.componentInstance.contentViewerAvailable = about?.contentViewerUrl != null;

                    dialogReference.componentInstance.downloadContent
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe(() => {
                            this.store.dispatch(
                                QueueListingActions.downloadFlowFileContent({
                                    request: {
                                        uri: request.flowfile.uri,
                                        clusterNodeId: request.clusterNodeId
                                    }
                                })
                            );
                        });

                    if (about) {
                        dialogReference.componentInstance.viewContent
                            .pipe(takeUntil(dialogReference.afterClosed()))
                            .subscribe(() => {
                                this.store.dispatch(
                                    QueueListingActions.viewFlowFileContent({
                                        request: {
                                            uri: request.flowfile.uri,
                                            mimeType: request.flowfile.mimeType,
                                            clusterNodeId: request.clusterNodeId
                                        }
                                    })
                                );
                            });
                    }
                })
            ),
        {
            dispatch: false
        }
    );

    downloadFlowFileContent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(QueueListingActions.downloadFlowFileContent),
                map((action) => action.request),
                tap((request) => this.queueService.downloadContent(request))
            ),
        { dispatch: false }
    );

    viewFlowFileContent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(QueueListingActions.viewFlowFileContent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectAbout).pipe(isDefinedAndNotNull())),
                tap(([request, about]) => {
                    this.queueService.viewContent(request, about.contentViewerUrl);
                })
            ),
        { dispatch: false }
    );

    queueListingApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(QueueListingActions.queueListingApiError),
            tap(() => {
                this.store.dispatch(QueueListingActions.stopPollingQueueListingRequest());
            }),
            switchMap(({ error }) =>
                of(ErrorActions.addBannerError({ errorContext: { errors: [error], context: ErrorContextKey.QUEUE } }))
            )
        )
    );
}
