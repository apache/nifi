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
import * as LineageActions from './lineage.actions';
import * as ProvenanceActions from '../provenance-event-listing/provenance-event-listing.actions';
import { asyncScheduler, catchError, from, interval, map, NEVER, of, switchMap, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ProvenanceService } from '../../service/provenance.service';
import { Lineage } from './index';
import { selectClusterNodeId } from '../provenance-event-listing/provenance-event-listing.selectors';
import { selectLineageId } from './lineage.selectors';

@Injectable()
export class LineageEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private provenanceService: ProvenanceService,
        private dialog: MatDialog
    ) {}

    submitLineageQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.submitLineageQuery),
            map((action) => action.request),
            switchMap((request) =>
                from(this.provenanceService.submitLineageQuery(request)).pipe(
                    map((response) =>
                        LineageActions.submitLineageQuerySuccess({
                            response: {
                                lineage: response.lineage
                            }
                        })
                    ),
                    catchError((error) => {
                        this.store.dispatch(
                            ProvenanceActions.showOkDialog({
                                title: 'Error',
                                message: error.error
                            })
                        );

                        return of(
                            LineageActions.lineageApiError({
                                error: error.error
                            })
                        );
                    })
                )
            )
        )
    );

    submitLineageQuerySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.submitLineageQuerySuccess),
            map((action) => action.response),
            switchMap((response) => {
                const query: Lineage = response.lineage;
                if (query.finished) {
                    this.dialog.closeAll();
                    return of(LineageActions.deleteLineageQuery());
                } else {
                    return of(LineageActions.startPollingLineageQuery());
                }
            })
        )
    );

    startPollingLineageQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.startPollingLineageQuery),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(LineageActions.stopPollingLineageQuery)))
                )
            ),
            switchMap(() => of(LineageActions.pollLineageQuery()))
        )
    );

    pollLineageQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.pollLineageQuery),
            concatLatestFrom(() => [this.store.select(selectLineageId), this.store.select(selectClusterNodeId)]),
            switchMap(([action, id, clusterNodeId]) => {
                if (id) {
                    return from(this.provenanceService.getLineageQuery(id, clusterNodeId)).pipe(
                        map((response) =>
                            LineageActions.pollLineageQuerySuccess({
                                response: {
                                    lineage: response.lineage
                                }
                            })
                        ),
                        catchError((error) =>
                            of(
                                LineageActions.lineageApiError({
                                    error: error.error
                                })
                            )
                        )
                    );
                } else {
                    return NEVER;
                }
            })
        )
    );

    pollLineageQuerySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.pollLineageQuerySuccess),
            map((action) => action.response),
            switchMap((response) => {
                const query: Lineage = response.lineage;
                if (query.finished) {
                    this.dialog.closeAll();
                    return of(LineageActions.stopPollingLineageQuery());
                } else {
                    return NEVER;
                }
            })
        )
    );

    stopPollingLineageQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.stopPollingLineageQuery),
            switchMap((response) => of(LineageActions.deleteLineageQuery()))
        )
    );

    deleteLineageQuery$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(LineageActions.deleteLineageQuery),
                concatLatestFrom(() => [this.store.select(selectLineageId), this.store.select(selectClusterNodeId)]),
                tap(([action, id, clusterNodeId]) => {
                    if (id) {
                        this.provenanceService.deleteLineageQuery(id, clusterNodeId).subscribe();
                    }
                })
            ),
        { dispatch: false }
    );
}
