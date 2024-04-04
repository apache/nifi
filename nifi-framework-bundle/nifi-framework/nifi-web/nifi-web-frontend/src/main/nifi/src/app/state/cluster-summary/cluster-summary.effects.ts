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
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, takeUntil } from 'rxjs';
import { ClusterService } from '../../service/cluster.service';
import { selectClusterSummary } from './cluster-summary.selectors';
import { isDefinedAndNotNull } from '../shared';
import { Store } from '@ngrx/store';
import { ClusterSummaryState } from './index';
import { HttpErrorResponse } from '@angular/common/http';
import * as ErrorActions from '../error/error.actions';

@Injectable()
export class ClusterSummaryEffects {
    constructor(
        private actions$: Actions,
        private clusterService: ClusterService,
        private store: Store<ClusterSummaryState>
    ) {}

    loadClusterSummary$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ClusterSummaryActions.loadClusterSummary),
            switchMap(() => {
                return from(
                    this.clusterService.getClusterSummary().pipe(
                        map((response) =>
                            ClusterSummaryActions.loadClusterSummarySuccess({
                                response
                            })
                        ),
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
