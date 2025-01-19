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
import * as LineageActions from './lineage.actions';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { ProvenanceService } from '../../service/provenance.service';
import { Lineage } from './index';
import { selectActiveLineageId, selectClusterNodeIdFromActiveLineage } from './lineage.selectors';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { isDefinedAndNotNull, NiFiCommon } from '@nifi/shared';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class LineageEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private provenanceService: ProvenanceService,
        private errorHelper: ErrorHelper,
        private nifiCommon: NiFiCommon,
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
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                LineageActions.lineageApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.store.dispatch(LineageActions.stopPollingLineageQuery());

                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
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
                if (query.finished || !this.nifiCommon.isEmpty(query.results.errors)) {
                    response.lineage.results.errors?.forEach((error) => {
                        this.store.dispatch(
                            ErrorActions.addBannerError({
                                errorContext: { errors: [error], context: ErrorContextKey.LINEAGE }
                            })
                        );
                    });

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
            concatLatestFrom(() => [
                this.store.select(selectActiveLineageId).pipe(isDefinedAndNotNull()),
                this.store.select(selectClusterNodeIdFromActiveLineage)
            ]),
            switchMap(([, id, clusterNodeId]) =>
                from(this.provenanceService.getLineageQuery(id, clusterNodeId)).pipe(
                    map((response) =>
                        LineageActions.pollLineageQuerySuccess({
                            response: {
                                lineage: response.lineage
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                LineageActions.lineageApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.store.dispatch(LineageActions.stopPollingLineageQuery());

                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    pollLineageQuerySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.pollLineageQuerySuccess),
            map((action) => action.response),
            filter(
                (response) => response.lineage.finished || !this.nifiCommon.isEmpty(response.lineage.results.errors)
            ),
            switchMap((response) => {
                response.lineage.results.errors?.forEach((error) => {
                    this.store.dispatch(
                        ErrorActions.addBannerError({
                            errorContext: {
                                errors: [error],
                                context: ErrorContextKey.LINEAGE
                            }
                        })
                    );
                });

                return of(LineageActions.stopPollingLineageQuery());
            })
        )
    );

    stopPollingLineageQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.stopPollingLineageQuery),
            switchMap(() => of(LineageActions.deleteLineageQuery()))
        )
    );

    deleteLineageQuery$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.deleteLineageQuery),
            concatLatestFrom(() => [
                this.store.select(selectActiveLineageId),
                this.store.select(selectClusterNodeIdFromActiveLineage)
            ]),
            tap(([, id, clusterNodeId]) => {
                this.dialog.closeAll();

                if (id) {
                    this.provenanceService.deleteLineageQuery(id, clusterNodeId).subscribe();
                }
            }),
            switchMap(() => of(LineageActions.deleteLineageQuerySuccess()))
        )
    );

    lineageApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LineageActions.lineageApiError),
            tap(() => {
                this.store.dispatch(LineageActions.stopPollingLineageQuery());
            }),
            switchMap(({ error }) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: {
                            errors: [error],
                            context: ErrorContextKey.LINEAGE
                        }
                    })
                )
            )
        )
    );
}
