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
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { asyncScheduler, catchError, from, interval, map, of, startWith, switchMap, takeUntil, tap } from 'rxjs';
import * as FlowAnalysisActions from './flow-analysis.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { FlowAnalysisService } from '../../service/flow-analysis.service';
import { ErrorHelper } from 'apps/nifi/src/app/service/error-helper.service';
import { selectCurrentProcessGroupId } from '../flow/flow.selectors';
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { LARGE_DIALOG } from '@nifi/shared';
import { ViolationDetailsDialogComponent } from '../../ui/canvas/header/flow-analysis-drawer/violation-details-dialog/violation-details-dialog.component';

@Injectable()
export class FlowAnalysisEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private flowAnalysisService: FlowAnalysisService,
        private errorHelper: ErrorHelper,
        private router: Router,
        private dialog: MatDialog
    ) {}

    startPollingFlowAnalysis$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisActions.startPollingFlowAnalysis),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(
                    startWith(0),
                    takeUntil(this.actions$.pipe(ofType(FlowAnalysisActions.stopPollingFlowAnalysis)))
                )
            ),
            switchMap(() => of(FlowAnalysisActions.pollFlowAnalysis()))
        )
    );

    resetPollingFlowAnalysis$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisActions.resetPollingFlowAnalysis),
            switchMap(() => {
                this.store.dispatch(FlowAnalysisActions.stopPollingFlowAnalysis());
                return of(FlowAnalysisActions.pollFlowAnalysis());
            })
        )
    );

    pollFlowAnalysis$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisActions.pollFlowAnalysis),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return from(this.flowAnalysisService.getResults(pgId)).pipe(
                    map((response) =>
                        FlowAnalysisActions.pollFlowAnalysisSuccess({
                            response: response
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.store.dispatch(FlowAnalysisActions.stopPollingFlowAnalysis());
                        return of(
                            FlowAnalysisActions.flowAnalysisApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                );
            })
        )
    );

    navigateToEditFlowAnalysisRule$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisActions.navigateToEditFlowAnalysisRule),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/settings', 'flow-analysis-rules', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openRuleDetailsDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisActions.openRuleDetailsDialog),
                tap(({ violation, rule }) => {
                    this.dialog.open(ViolationDetailsDialogComponent, {
                        ...LARGE_DIALOG,
                        data: {
                            violation,
                            rule
                        }
                    });
                })
            ),
        { dispatch: false }
    );
}
