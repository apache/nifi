/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ClusterSummaryService } from '../../service/cluster-summary.service';
import { ProcessGroupStatusService } from '../../service/process-group-status.service';
import * as SummaryListingActions from './summary-listing.actions';
import * as StatusHistoryActions from '../../../../state/status-history/status-history.actions';

import { catchError, combineLatest, filter, map, of, switchMap, tap } from 'rxjs';
import { Router } from '@angular/router';
import { ComponentType } from '../../../../state/shared';

@Injectable()
export class SummaryListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private clusterSummaryService: ClusterSummaryService,
        private pgStatusService: ProcessGroupStatusService,
        private router: Router
    ) {}

    loadSummaryListing$ = createEffect(() =>
        this.actions$.pipe(
            ofType(SummaryListingActions.loadSummaryListing),
            map((action) => action.recursive),
            switchMap((recursive) =>
                combineLatest([
                    this.clusterSummaryService.getClusterSummary(),
                    this.pgStatusService.getProcessGroupsStatus(recursive)
                ]).pipe(
                    map(([clusterSummary, status]) =>
                        SummaryListingActions.loadSummaryListingSuccess({
                            response: {
                                clusterSummary,
                                status
                            }
                        })
                    ),
                    catchError((error) => of(SummaryListingActions.summaryListingApiError({ error: error.error })))
                )
            )
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

    completeProcessorStatusHistory$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(StatusHistoryActions.viewStatusHistoryComplete),
                map((action) => action.request),
                filter((request) => request.source === 'summary' && request.componentType === ComponentType.Processor),
                tap((request) => {
                    this.store.dispatch(
                        SummaryListingActions.selectProcessorStatus({
                            request: {
                                id: request.componentId
                            }
                        })
                    );
                })
            ),
        { dispatch: false }
    );
}
