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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import * as BulletinBoardActions from './bulletin-board.actions';
import { asyncScheduler, catchError, from, interval, map, of, switchMap, takeUntil } from 'rxjs';
import { BulletinBoardService } from '../../service/bulletin-board.service';
import { selectBulletinBoardFilter, selectLastBulletinId, selectLoadedTimestamp } from './bulletin-board.selectors';
import { LoadBulletinBoardRequest } from './index';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { initialBulletinBoardState } from './bulletin-board.reducer';

@Injectable()
export class BulletinBoardEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private bulletinBoardService = inject(BulletinBoardService);
    private errorHelper = inject(ErrorHelper);

    loadBulletinBoard$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulletinBoardActions.loadBulletinBoard),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectLoadedTimestamp)),
            switchMap(([request, loadedTimestamp]) =>
                from(
                    this.bulletinBoardService.getBulletins(request).pipe(
                        map((response: any) =>
                            BulletinBoardActions.loadBulletinBoardSuccess({
                                response: {
                                    bulletinBoard: response.bulletinBoard,
                                    loadedTimestamp: response.bulletinBoard.generated
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                BulletinBoardActions.loadBulletinBoardError({
                                    errorResponse,
                                    loadedTimestamp,
                                    status:
                                        loadedTimestamp !== initialBulletinBoardState.loadedTimestamp
                                            ? 'success'
                                            : 'pending'
                                })
                            )
                        )
                    )
                )
            )
        )
    );

    bulletinBoardListingError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulletinBoardActions.loadBulletinBoardError),
            map((action) =>
                this.errorHelper.handleLoadingError(
                    action.loadedTimestamp !== initialBulletinBoardState.loadedTimestamp,
                    action.errorResponse
                )
            )
        )
    );

    setBulletinBoardAutoRefresh$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulletinBoardActions.setBulletinBoardAutoRefresh),
            map((action) => action.autoRefresh),
            switchMap((autoRefresh) => {
                if (autoRefresh) {
                    return of(BulletinBoardActions.startBulletinBoardPolling());
                }
                return of(BulletinBoardActions.stopBulletinBoardPolling());
            })
        )
    );

    startBulletinBoardPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BulletinBoardActions.startBulletinBoardPolling),
            switchMap(() =>
                interval(3000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(BulletinBoardActions.stopBulletinBoardPolling)))
                )
            ),
            concatLatestFrom(() => [
                this.store.select(selectBulletinBoardFilter),
                this.store.select(selectLastBulletinId)
            ]),
            switchMap(([, filter, lastBulletinId]) => {
                const request: LoadBulletinBoardRequest = {};
                if (lastBulletinId > 0) {
                    request.after = lastBulletinId;
                }
                if (filter.filterTerm.length > 0) {
                    const filterTerm = filter.filterTerm;
                    switch (filter.filterColumn) {
                        case 'message':
                            request.message = filterTerm;
                            break;
                        case 'id':
                            request.sourceId = filterTerm;
                            break;
                        case 'groupId':
                            request.groupId = filterTerm;
                            break;
                        case 'name':
                            request.sourceName = filterTerm;
                            break;
                    }
                }
                return of(BulletinBoardActions.loadBulletinBoard({ request }));
            })
        )
    );
}
