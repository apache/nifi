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
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import * as ClusterSummaryActions from './cluster-summary.actions';
import { asyncScheduler, catchError, delay, filter, from, interval, map, of, switchMap, takeUntil, tap } from 'rxjs';
import { ClusterService } from '../../service/cluster.service';
import { selectClusterSummary } from './cluster-summary.selectors';
import { isDefinedAndNotNull } from '../shared';
import { Store } from '@ngrx/store';
import { ClusterSummary, ClusterSummaryState } from './index';
import { HttpErrorResponse } from '@angular/common/http';
import * as ErrorActions from '../error/error.actions';
import { acknowledgeClusterConnectionChange, setDisconnectionAcknowledged } from './cluster-summary.actions';
import { OkDialog } from '../../ui/common/ok-dialog/ok-dialog.component';
import { MEDIUM_DIALOG } from '../../index';
import { MatDialog } from '@angular/material/dialog';

@Injectable()
export class ClusterSummaryEffects {
    constructor(
        private actions$: Actions,
        private clusterService: ClusterService,
        private store: Store<ClusterSummaryState>,
        private dialog: MatDialog
    ) {}

    loadClusterSummary$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterSummaryActions.loadClusterSummary),
            concatLatestFrom(() => this.store.select(selectClusterSummary)),
            switchMap(([, currentClusterSummary]) => {
                return from(
                    this.clusterService.getClusterSummary().pipe(
                        map((response) => {
                            const clusterSummary: ClusterSummary = response.clusterSummary;
                            const connectedToCluster = clusterSummary.connectedToCluster;

                            if (currentClusterSummary) {
                                if (currentClusterSummary.connectedToCluster !== clusterSummary.connectedToCluster) {
                                    this.store.dispatch(acknowledgeClusterConnectionChange({ connectedToCluster }));
                                }
                            } else {
                                if (clusterSummary.clustered && !clusterSummary.connectedToCluster) {
                                    this.store.dispatch(acknowledgeClusterConnectionChange({ connectedToCluster }));
                                }
                            }

                            return ClusterSummaryActions.loadClusterSummarySuccess({
                                response
                            });
                        }),
                        catchError((error) => of(ClusterSummaryActions.clusterSummaryApiError({ error: error.error })))
                    )
                );
            })
        )
    );

    startProcessGroupPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterSummaryActions.startClusterSummaryPolling),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(ClusterSummaryActions.stopClusterSummaryPolling)))
                )
            ),
            switchMap(() => of(ClusterSummaryActions.loadClusterSummary()))
        )
    );

    acknowledgeClusterConnectionChange$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ClusterSummaryActions.acknowledgeClusterConnectionChange),
                delay(2000), // minor delay to allow component at the desired route to act first
                tap(({ connectedToCluster }) => {
                    const message = connectedToCluster
                        ? 'This node just joined the cluster. Any modifications to the data flow made here will replicate across the cluster.'
                        : 'This node is currently not connected to the cluster. Any modifications to the data flow made here will not replicate across the cluster.';

                    const dialogReference = this.dialog.open(OkDialog, {
                        ...MEDIUM_DIALOG,
                        disableClose: true,
                        data: {
                            title: 'Cluster Connection',
                            message
                        }
                    });

                    dialogReference.componentInstance.ok
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe(() => {
                            if (connectedToCluster) {
                                // this node has rejoined the cluster and any previous acknowledged disconnection can be reset
                                this.store.dispatch(setDisconnectionAcknowledged({ disconnectionAcknowledged: false }));
                            } else {
                                // this node is not currently connected so the user has acknowledged the disconnection
                                this.store.dispatch(setDisconnectionAcknowledged({ disconnectionAcknowledged: true }));
                            }
                        });
                })
            ),
        { dispatch: false }
    );

    searchCluster$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterSummaryActions.searchCluster),
            map((action) => action.request),
            concatLatestFrom(() =>
                this.store.select(selectClusterSummary).pipe(
                    isDefinedAndNotNull(),
                    filter((clusterSummary) => clusterSummary.connectedToCluster)
                )
            ),
            switchMap(([request]) => {
                return from(this.clusterService.searchCluster(request.q)).pipe(
                    map((response) =>
                        ClusterSummaryActions.searchClusterSuccess({
                            response: response
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(ErrorActions.snackBarError({ error: errorResponse.error }))
                    )
                );
            })
        )
    );
}
