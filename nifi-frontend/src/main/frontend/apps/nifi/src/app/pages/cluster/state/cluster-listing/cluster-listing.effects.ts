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
import { ActionCreator, Creator, Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { Router } from '@angular/router';
import * as ClusterListingActions from './cluster-listing.actions';
import { catchError, filter, from, map, of, switchMap, take, tap } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { selectClusterListingStatus } from './cluster-listing.selectors';
import { reloadSystemDiagnostics } from '../../../../state/system-diagnostics/system-diagnostics.actions';
import { ClusterService } from '../../service/cluster.service';
import { MatDialog } from '@angular/material/dialog';
import { LARGE_DIALOG, MEDIUM_DIALOG, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { ClusterNodeDetailDialog } from '../../ui/cluster-node-listing/cluster-node-detail-dialog/cluster-node-detail-dialog.component';
import * as ErrorActions from '../../../../state/error/error.actions';
import { SelectClusterNodeRequest } from './index';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { concatLatestFrom } from '@ngrx/operators';

@Injectable()
export class ClusterListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private errorHelper: ErrorHelper,
        private router: Router,
        private clusterService: ClusterService,
        private dialog: MatDialog
    ) {}

    loadClusterListing$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.loadClusterListing),
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

    loadClusterListingSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterListingActions.loadClusterListingSuccess),
            concatLatestFrom(() => this.store.select(selectCurrentUser)),
            filter(([, currentUser]) => currentUser.systemPermissions.canRead),
            map(() =>
                reloadSystemDiagnostics({
                    request: {
                        nodewise: true,
                        errorStrategy: 'banner'
                    }
                })
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
                        return ClusterListingActions.updateNodeSuccess({ response: entity });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        return of(ClusterListingActions.clusterNodeSnackbarError({ errorResponse }));
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
                        return ClusterListingActions.updateNodeSuccess({ response: entity });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        return of(ClusterListingActions.clusterNodeSnackbarError({ errorResponse }));
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
                        return ClusterListingActions.updateNodeSuccess({ response: entity });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        return of(ClusterListingActions.clusterNodeSnackbarError({ errorResponse }));
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
                        return ClusterListingActions.removeNodeSuccess({ response: request });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        return of(ClusterListingActions.clusterNodeSnackbarError({ errorResponse }));
                    })
                )
            )
        )
    );

    private selectClusterNode = (
        action: ActionCreator<any, Creator<any, { request: SelectClusterNodeRequest }>>,
        path: string
    ) =>
        createEffect(
            () =>
                this.actions$.pipe(
                    ofType(action),
                    map((action) => action.request),
                    tap((request) => {
                        if (request.repository) {
                            this.router.navigate(['/cluster', path, request.id, request.repository]);
                        } else {
                            this.router.navigate(['/cluster', path, request.id]);
                        }
                    })
                ),
            { dispatch: false }
        );

    selectClusterNode$ = this.selectClusterNode(ClusterListingActions.selectClusterNode, 'nodes');
    selectSystemNode$ = this.selectClusterNode(ClusterListingActions.selectSystemNode, 'system');
    selectJvmNode$ = this.selectClusterNode(ClusterListingActions.selectJvmNode, 'jvm');
    selectFlowFileStorageNode$ = this.selectClusterNode(
        ClusterListingActions.selectFlowFileStorageNode,
        'flowfile-storage'
    );
    selectContentStorageNode$ = this.selectClusterNode(
        ClusterListingActions.selectContentStorageNode,
        'content-storage'
    );
    selectProvenanceStorageNode$ = this.selectClusterNode(
        ClusterListingActions.selectProvenanceStorageNode,
        'provenance-storage'
    );
    selectVersionNode$ = this.selectClusterNode(ClusterListingActions.selectVersionNode, 'versions');

    private clearNodeSelection = (action: ActionCreator, path: string) =>
        createEffect(
            () =>
                this.actions$.pipe(
                    ofType(action),
                    tap(() => {
                        this.router.navigate(['/cluster', path]);
                    })
                ),
            { dispatch: false }
        );

    clearClusterNodeSelection$ = this.clearNodeSelection(ClusterListingActions.clearClusterNodeSelection, 'nodes');
    clearSystemNodeSelection$ = this.clearNodeSelection(ClusterListingActions.clearSystemNodeSelection, 'system');
    clearJvmNodeSelection$ = this.clearNodeSelection(ClusterListingActions.clearJvmNodeSelection, 'jvm');
    clearFlowFileStorageNodeSelection$ = this.clearNodeSelection(
        ClusterListingActions.clearFlowFileStorageNodeSelection,
        'flowfile-storage'
    );
    clearContentStorageNodeSelection$ = this.clearNodeSelection(
        ClusterListingActions.clearContentStorageNodeSelection,
        'content-storage'
    );
    clearProvenanceStorageNodeSelection$ = this.clearNodeSelection(
        ClusterListingActions.clearProvenanceStorageNodeSelection,
        'provenance-storage'
    );
    clearVersionsNodeSelection$ = this.clearNodeSelection(ClusterListingActions.clearVersionsNodeSelection, 'versions');

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
            map((action) => action.errorResponse),
            switchMap((errorResponse) =>
                of(ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) }))
            )
        )
    );

    navigateToClusterNodeListing$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.navigateToClusterNodeListing),
                tap(() => {
                    this.router.navigate(['/cluster']);
                })
            ),
        { dispatch: false }
    );

    navigateHome$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterListingActions.navigateHome),
                tap(() => {
                    this.router.navigate(['/']);
                })
            ),
        { dispatch: false }
    );
}
