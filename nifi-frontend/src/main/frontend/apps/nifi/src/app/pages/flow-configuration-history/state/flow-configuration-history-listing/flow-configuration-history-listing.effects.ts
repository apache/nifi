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
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import * as HistoryActions from './flow-configuration-history-listing.actions';
import { catchError, from, map, of, switchMap, take, tap } from 'rxjs';
import { selectHistoryQuery, selectHistoryStatus } from './flow-configuration-history-listing.selectors';
import { FlowConfigurationHistoryService } from '../../service/flow-configuration-history.service';
import { HistoryEntity } from './index';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { ActionDetails } from '../../ui/flow-configuration-history-listing/action-details/action-details.component';
import { PurgeHistory } from '../../ui/flow-configuration-history-listing/purge-history/purge-history.component';
import { isDefinedAndNotNull, MEDIUM_DIALOG, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import * as ErrorActions from '../../../../state/error/error.actions';
import { selectAbout } from '../../../../state/about/about.selectors';

@Injectable()
export class FlowConfigurationHistoryListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private historyService: FlowConfigurationHistoryService,
        private router: Router
    ) {}

    loadHistory$ = createEffect(() =>
        this.actions$.pipe(
            ofType(HistoryActions.loadHistory),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectHistoryStatus)),
            switchMap(([request, status]) =>
                from(this.historyService.getHistory(request)).pipe(
                    map((response: HistoryEntity) =>
                        HistoryActions.loadHistorySuccess({
                            response: response
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                )
            )
        )
    );

    flowConfigurationHistorySnackbarError = createEffect(() =>
        this.actions$.pipe(
            ofType(HistoryActions.flowConfigurationHistorySnackbarError),
            map((action) => action.errorResponse),
            switchMap((errorResponse) =>
                of(ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) }))
            )
        )
    );

    openMoreDetailsDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(HistoryActions.openMoreDetailsDialog),
                map((action) => action.request),
                tap((actionEntity) => {
                    this.dialog.open(ActionDetails, {
                        ...MEDIUM_DIALOG,
                        data: actionEntity
                    });
                })
            ),
        { dispatch: false }
    );

    openPurgeHistoryDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(HistoryActions.openPurgeHistoryDialog),
                tap(() => {
                    const dialogReference = this.dialog.open(PurgeHistory, {
                        ...MEDIUM_DIALOG,
                        minWidth: 600,
                        maxWidth: 600
                    });

                    dialogReference.componentInstance.submitPurgeRequest
                        .pipe(
                            isDefinedAndNotNull(),
                            concatLatestFrom(() => this.store.select(selectAbout).pipe(isDefinedAndNotNull())),
                            take(1)
                        )
                        .subscribe(([result, about]) => {
                            const yesNoRef = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Confirm History Purge',
                                    message: `Are you sure you want to delete all history before '${result.endDate} ${about.timezone}'?`
                                }
                            });

                            yesNoRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                this.store.dispatch(HistoryActions.purgeHistory({ request: { ...result } }));
                            });
                        });
                })
            ),
        {
            dispatch: false
        }
    );

    purgeHistory$ = createEffect(() =>
        this.actions$.pipe(
            ofType(HistoryActions.purgeHistory),
            map((action) => action.request),
            switchMap((request) =>
                from(this.historyService.purgeHistory(request)).pipe(
                    map(() => HistoryActions.purgeHistorySuccess()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(HistoryActions.flowConfigurationHistorySnackbarError({ errorResponse }))
                    )
                )
            )
        )
    );

    purgeHistorySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(HistoryActions.purgeHistorySuccess),
            concatLatestFrom(() => this.store.select(selectHistoryQuery)),
            switchMap(([, query]) => {
                if (query) {
                    return of(HistoryActions.loadHistory({ request: { ...query } }));
                }
                return of(HistoryActions.loadHistory({ request: { count: 50, offset: 0 } }));
            })
        )
    );
}
