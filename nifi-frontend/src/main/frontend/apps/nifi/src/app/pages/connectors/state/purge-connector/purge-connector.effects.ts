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
import { Store } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { isDefinedAndNotNull, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { NiFiState } from '../../../../state';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { CancelDialog } from '../../../../ui/common/cancel-dialog/cancel-dialog.component';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { DropRequest } from '../../../flow-designer/state/queue';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import { selectPurgeConnectorId, selectPurgeDropRequestEntity } from './purge-connector.selectors';
import { loadConnectorsListing } from '../connectors-listing/connectors-listing.actions';
import {
    deletePurgeRequest,
    pollPurgeConnector,
    pollPurgeConnectorSuccess,
    promptPurgeConnector,
    purgeConnectorApiError,
    resetPurgeConnectorState,
    showPurgeConnectorResults,
    startPollingPurgeConnector,
    stopPollingPurgeConnector,
    submitPurgeConnector,
    submitPurgeConnectorSuccess
} from './purge-connector.actions';

@Injectable()
export class PurgeConnectorEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);
    private dialog = inject(MatDialog);

    promptPurgeConnector$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(promptPurgeConnector),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open<YesNoDialog>(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Purge Connector',
                            message: `Are you sure you want to purge connector '${request.connector.component.name}'? All FlowFiles waiting at the time of the request will be removed.`
                        }
                    });

                    dialogRef.componentInstance!.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(submitPurgeConnector({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    submitPurgeConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(submitPurgeConnector),
            map((action) => action.request),
            switchMap((request) => {
                const dialogReference = this.dialog.open(CancelDialog, {
                    data: {
                        title: 'Purge Connector',
                        message: 'Waiting for connector to be purged...'
                    },
                    disableClose: true
                });

                dialogReference.componentInstance.exit.pipe(take(1)).subscribe(() => {
                    this.store.dispatch(stopPollingPurgeConnector());
                });

                return from(this.connectorService.createPurgeRequest(request.connector.id)).pipe(
                    map((response) =>
                        submitPurgeConnectorSuccess({
                            response: { dropEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            purgeConnectorApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                );
            })
        )
    );

    submitPurgeConnectorSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(submitPurgeConnectorSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const dropRequest: DropRequest = response.dropEntity.dropRequest;
                if (dropRequest.finished) {
                    return of(deletePurgeRequest());
                } else {
                    return of(startPollingPurgeConnector());
                }
            })
        )
    );

    startPollingPurgeConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(startPollingPurgeConnector),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(takeUntil(this.actions$.pipe(ofType(stopPollingPurgeConnector))))
            ),
            switchMap(() => of(pollPurgeConnector()))
        )
    );

    pollPurgeConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(pollPurgeConnector),
            concatLatestFrom(() => [
                this.store.select(selectPurgeDropRequestEntity).pipe(isDefinedAndNotNull()),
                this.store.select(selectPurgeConnectorId)
            ]),
            switchMap(([, dropEntity, connectorId]) => {
                if (!connectorId) {
                    return of(purgeConnectorApiError({ error: 'Purge request state is missing' }));
                }

                return from(this.connectorService.getPurgeRequest(connectorId, dropEntity.dropRequest.id)).pipe(
                    map((response) =>
                        pollPurgeConnectorSuccess({
                            response: { dropEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            purgeConnectorApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                );
            })
        )
    );

    pollPurgeConnectorSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(pollPurgeConnectorSuccess),
            map((action) => action.response),
            filter((response) => response.dropEntity.dropRequest.finished),
            switchMap(() => of(stopPollingPurgeConnector()))
        )
    );

    stopPollingPurgeConnector$ = createEffect(() =>
        this.actions$.pipe(
            ofType(stopPollingPurgeConnector),
            switchMap(() => of(deletePurgeRequest()))
        )
    );

    deletePurgeRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(deletePurgeRequest),
            concatLatestFrom(() => [
                this.store.select(selectPurgeDropRequestEntity).pipe(isDefinedAndNotNull()),
                this.store.select(selectPurgeConnectorId)
            ]),
            switchMap(([, dropEntity, connectorId]) => {
                this.dialog.closeAll();

                if (!connectorId) {
                    return of(loadConnectorsListing());
                }

                return from(this.connectorService.deletePurgeRequest(connectorId, dropEntity.dropRequest.id)).pipe(
                    map((response) =>
                        showPurgeConnectorResults({
                            request: { dropEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.store.dispatch(
                            purgeConnectorApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                        return of(
                            showPurgeConnectorResults({
                                request: { dropEntity }
                            })
                        );
                    })
                );
            })
        )
    );

    showPurgeConnectorResults$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(showPurgeConnectorResults),
                map((action) => action.request),
                tap((request) => {
                    const dropRequest: DropRequest = request.dropEntity.dropRequest;
                    let message: string;

                    if (dropRequest.dropped) {
                        const droppedTokens: string[] = dropRequest.dropped.split(/ \/ /);
                        message = `${droppedTokens[0]} FlowFiles (${droppedTokens[1]})`;

                        if (dropRequest.percentCompleted < 100) {
                            const originalTokens: string[] = dropRequest.original.split(/ \/ /);
                            message = `${message} out of ${originalTokens[0]} (${originalTokens[1]})`;
                        }

                        message = `${message} were removed from the connector.`;
                    } else {
                        message = 'Purge request completed.';
                    }

                    if (dropRequest.failureReason) {
                        message = `${message} ${dropRequest.failureReason}`;
                    }

                    const dialogReference = this.dialog.open(OkDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Purge Connector',
                            message
                        }
                    });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(resetPurgeConnectorState());
                    });

                    this.store.dispatch(loadConnectorsListing());
                })
            ),
        { dispatch: false }
    );

    purgeConnectorApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(purgeConnectorApiError),
            map((action) => action.error),
            tap(() => {
                this.dialog.closeAll();
                this.store.dispatch(resetPurgeConnectorState());
            }),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.CONNECTORS }
                    })
                )
            )
        )
    );
}
