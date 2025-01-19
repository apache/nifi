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

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ProcessGroupStatusService } from '../../service/process-group-status.service';
import * as SummaryListingActions from './summary-listing.actions';
import * as StatusHistoryActions from '../../../../state/status-history/status-history.actions';
import { catchError, filter, from, map, of, switchMap, tap } from 'rxjs';
import { Router } from '@angular/router';
import { ComponentType } from '@nifi/shared';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { selectSelectedClusterNode, selectSummaryListingStatus } from './summary-listing.selectors';
import { LoadSummaryRequest } from './index';

@Injectable()
export class SummaryListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private pgStatusService: ProcessGroupStatusService,
        private errorHelper: ErrorHelper,
        private router: Router
    ) {}

    loadSummaryListing$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SummaryListingActions.loadSummaryListing),
            map((action) => action.recursive),
            concatLatestFrom(() => [
                this.store.select(selectSummaryListingStatus),
                this.store.select(selectSelectedClusterNode)
            ]),
            switchMap(([recursive, listingStatus, selectedClusterNode]) => {
                const request: LoadSummaryRequest = {
                    recursive
                };
                if (selectedClusterNode) {
                    request.clusterNodeId = selectedClusterNode.id;
                }
                return from(this.pgStatusService.getProcessGroupsStatus(request)).pipe(
                    map((status) =>
                        SummaryListingActions.loadSummaryListingSuccess({
                            response: {
                                status
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(listingStatus, errorResponse))
                    )
                );
            })
        )
    );

    selectProcessorStatus$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.selectProcessorStatus),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/summary', 'processors', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearProcessorStatusSelection$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.clearProcessorStatusSelection),
                tap(() => {
                    this.router.navigate(['/summary', 'processors']);
                })
            ),
        { dispatch: false }
    );

    selectProcessGroupStatus$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.selectProcessGroupStatus),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/summary', 'process-groups', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearProcessGroupStatusSelection$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.clearProcessGroupStatusSelection),
                tap(() => {
                    this.router.navigate(['/summary', 'process-groups']);
                })
            ),
        { dispatch: false }
    );

    selectInputPortStatus$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.selectInputPortStatus),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/summary', 'input-ports', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearInputPortStatusSelection$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.clearInputPortStatusSelection),
                tap(() => {
                    this.router.navigate(['/summary', 'input-ports']);
                })
            ),
        { dispatch: false }
    );

    selectOutputPortStatus$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.selectOutputPortStatus),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/summary', 'output-ports', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearOutputPortStatusSelection$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.clearOutputPortStatusSelection),
                tap(() => {
                    this.router.navigate(['/summary', 'output-ports']);
                })
            ),
        { dispatch: false }
    );

    selectConnectionStatus$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.selectConnectionStatus),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/summary', 'connections', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearConnectionStatusSelection$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.clearConnectionStatusSelection),
                tap(() => {
                    this.router.navigate(['/summary', 'connections']);
                })
            ),
        { dispatch: false }
    );

    selectRpgStatus$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.selectRemoteProcessGroupStatus),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/summary', 'remote-process-groups', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearRpgStatusSelection = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.clearRemoteProcessGroupStatusSelection),
                tap(() => {
                    this.router.navigate(['/summary', 'remote-process-groups']);
                })
            ),
        { dispatch: false }
    );

    navigateToProcessorStatusHistory$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.navigateToViewProcessorStatusHistory),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/summary', 'processors', id, 'history']);
                })
            ),
        { dispatch: false }
    );

    navigateToViewProcessGroupStatusHistory$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.navigateToViewProcessGroupStatusHistory),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/summary', 'process-groups', id, 'history']);
                })
            ),
        { dispatch: false }
    );

    navigateToViewConnectionStatusHistory$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.navigateToViewConnectionStatusHistory),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/summary', 'connections', id, 'history']);
                })
            ),
        { dispatch: false }
    );

    navigateToViewRpgStatusHistory$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(SummaryListingActions.navigateToViewRemoteProcessGroupStatusHistory),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/summary', 'remote-process-groups', id, 'history']);
                })
            ),
        { dispatch: false }
    );

    // update the route to remove "/history", selecting the component in the summary list
    completeStatusHistory$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(StatusHistoryActions.viewStatusHistoryComplete),
                map((action) => action.request),
                filter((request) => request.source === 'summary'),
                tap((request) => {
                    switch (request.componentType) {
                        case ComponentType.ProcessGroup:
                            this.store.dispatch(
                                SummaryListingActions.selectProcessGroupStatus({
                                    request: {
                                        id: request.componentId
                                    }
                                })
                            );
                            break;
                        case ComponentType.Connection:
                            this.store.dispatch(
                                SummaryListingActions.selectConnectionStatus({
                                    request: {
                                        id: request.componentId
                                    }
                                })
                            );
                            break;
                        case ComponentType.RemoteProcessGroup:
                            this.store.dispatch(
                                SummaryListingActions.selectRemoteProcessGroupStatus({
                                    request: {
                                        id: request.componentId
                                    }
                                })
                            );
                            break;
                        case ComponentType.Processor:
                        default:
                            this.store.dispatch(
                                SummaryListingActions.selectProcessorStatus({
                                    request: {
                                        id: request.componentId
                                    }
                                })
                            );
                    }
                })
            ),
        { dispatch: false }
    );
}
