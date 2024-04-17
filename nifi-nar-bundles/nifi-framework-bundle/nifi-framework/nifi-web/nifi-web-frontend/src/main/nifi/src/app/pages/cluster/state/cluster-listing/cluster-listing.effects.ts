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
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { Router } from '@angular/router';
import * as ClusterListingActions from './cluster-listing.actions';
import { catchError, from, map, of, switchMap, take, tap } from 'rxjs';
import { SystemDiagnosticsService } from '../../../../service/system-diagnostics.service';
import { HttpErrorResponse } from '@angular/common/http';
import { selectClusterListingStatus } from './cluster-listing.selectors';
import { reloadSystemDiagnostics } from '../../../../state/system-diagnostics/system-diagnostics.actions';
import { ClusterService } from '../../service/cluster.service';
import { MatDialog } from '@angular/material/dialog';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { LARGE_DIALOG, MEDIUM_DIALOG, SMALL_DIALOG } from '../../../../index';
import { ClusterNodeDetailDialog } from '../../ui/cluster-node-listing/cluster-node-detail-dialog/cluster-node-detail-dialog.component';
import * as ErrorActions from '../../../../state/error/error.actions';

@Injectable()
export class ClusterListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private errorHelper: ErrorHelper,
        private router: Router,
        private systemDiagnosticsService: SystemDiagnosticsService,
        private clusterService: ClusterService,
        private dialog: MatDialog
    ) {}

    loadClusterListing$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.loadClusterListing),
            tap(() => {
                this.store.dispatch(reloadSystemDiagnostics({ request: { nodewise: true } }));
            }),
            concatLatestFrom(() => [this.store.select(selectClusterListingStatus)]),
            switchMap(([, listingStatus]) =>
                from(this.clusterService.getClusterListing()).pipe(
                    map((response) => ClusterListingActions.loadClusterListingSuccess({ response: response.cluster })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(listingStatus, errorResponse))
                    )
                )
            )
        )
    );

    confirmAndDisconnectNode$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.confirmAndDisconnectNode),
                map((action) => action.request),
                tap((request) => {
                    const nodeAddress = `${request.address}:${request.apiPort}`;
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Disconnect Node',
                            message: `Disconnect '${nodeAddress}' from the cluster?`
                        }
                    });
                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(ClusterListingActions.disconnectNode({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    disconnectNode$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.disconnectNode),
            map((action) => action.request),
            switchMap((request) =>
                from(this.clusterService.disconnectNode(request.nodeId)).pipe(
                    map((entity) => {
                        this.dialog.closeAll();
                        return ClusterListingActions.updateNodeSuccess({ response: entity });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(ClusterListingActions.clusterNodeSnackbarError({ error: errorResponse.error }));
                    })
                )
            )
        )
    );

    confirmAndConnectNode$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.confirmAndConnectNode),
                map((action) => action.request),
                tap((request) => {
                    const nodeAddress = `${request.address}:${request.apiPort}`;
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Connect Node',
                            message: `Connect '${nodeAddress}' to the cluster?`
                        }
                    });
                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(ClusterListingActions.connectNode({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    connectNode$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.connectNode),
            map((action) => action.request),
            switchMap((request) =>
                from(this.clusterService.connectNode(request.nodeId)).pipe(
                    map((entity) => {
                        this.dialog.closeAll();
                        return ClusterListingActions.updateNodeSuccess({ response: entity });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(ClusterListingActions.clusterNodeSnackbarError({ error: errorResponse.error }));
                    })
                )
            )
        )
    );

    confirmAndOffloadNode$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.confirmAndOffloadNode),
                map((action) => action.request),
                tap((request) => {
                    const nodeAddress = `${request.address}:${request.apiPort}`;
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Offload Node',
                            message: `Offload '${nodeAddress}'?`
                        }
                    });
                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(ClusterListingActions.offloadNode({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    offloadNode$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.offloadNode),
            map((action) => action.request),
            switchMap((request) =>
                from(this.clusterService.offloadNode(request.nodeId)).pipe(
                    map((entity) => {
                        this.dialog.closeAll();
                        return ClusterListingActions.updateNodeSuccess({ response: entity });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(ClusterListingActions.clusterNodeSnackbarError({ error: errorResponse.error }));
                    })
                )
            )
        )
    );

    confirmAndRemoveNode$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.confirmAndRemoveNode),
                map((action) => action.request),
                tap((request) => {
                    const nodeAddress = `${request.address}:${request.apiPort}`;
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Remove Node',
                            message: `Remove '${nodeAddress}' from the cluster?`
                        }
                    });
                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(ClusterListingActions.removeNode({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    removeNode$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.removeNode),
            map((action) => action.request),
            switchMap((request) =>
                from(this.clusterService.removeNode(request.nodeId)).pipe(
                    map(() => {
                        this.dialog.closeAll();
                        return ClusterListingActions.removeNodeSuccess({ response: request });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(ClusterListingActions.clusterNodeSnackbarError({ error: errorResponse.error }));
                    })
                )
            )
        )
    );

    selectClusterNode$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.selectClusterNode),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/cluster', 'nodes', request.id]);
                })
            ),
        { dispatch: false }
    );

    clearClusterNodeSelection$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.clearClusterNodeSelection),
                tap(() => {
                    this.router.navigate(['/cluster', 'nodes']);
                })
            ),
        { dispatch: false }
    );

    showClusterNodeDetails$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.showClusterNodeDetails),
                map((action) => action.request),
                tap((request) => {
                    const dimensions = request.events.length > 0 ? LARGE_DIALOG : MEDIUM_DIALOG;
                    this.dialog.open(ClusterNodeDetailDialog, {
                        ...dimensions,
                        data: request
                    });
                })
            ),
        { dispatch: false }
    );

    clusterNodeSnackbarError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.clusterNodeSnackbarError),
            map((action) => action.error),
            switchMap((errorResponse) => of(ErrorActions.snackBarError({ error: errorResponse })))
        )
    );
}
