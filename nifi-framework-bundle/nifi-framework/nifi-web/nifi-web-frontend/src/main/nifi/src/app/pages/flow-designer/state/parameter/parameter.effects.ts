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
import * as ParameterActions from './parameter.actions';
import { Store } from '@ngrx/store';
import { CanvasState } from '../index';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, takeUntil } from 'rxjs';
import { isDefinedAndNotNull, ParameterContextUpdateRequest } from '../../../../state/shared';
import { selectUpdateRequest } from './parameter.selectors';
import { ParameterService } from '../../service/parameter.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';

@Injectable()
export class ParameterEffects {
    constructor(
        private actions$: Actions,
        private store: Store<CanvasState>,
        private parameterService: ParameterService,
        private errorHelper: ErrorHelper
    ) {}

    submitParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.submitParameterContextUpdateRequest),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterService.submitParameterContextUpdate(request)).pipe(
                    map((response) =>
                        ParameterActions.submitParameterContextUpdateRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ParameterActions.parameterApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    submitParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.submitParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const updateRequest: ParameterContextUpdateRequest = response.requestEntity.request;
                if (updateRequest.complete) {
                    return of(ParameterActions.deleteParameterContextUpdateRequest());
                } else {
                    return of(ParameterActions.startPollingParameterContextUpdateRequest());
                }
            })
        )
    );

    startPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.startPollingParameterContextUpdateRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(ParameterActions.stopPollingParameterContextUpdateRequest)))
                )
            ),
            switchMap(() => of(ParameterActions.pollParameterContextUpdateRequest()))
        )
    );

    pollParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.pollParameterContextUpdateRequest),
            concatLatestFrom(() => this.store.select(selectUpdateRequest).pipe(isDefinedAndNotNull())),
            switchMap(([, updateRequest]) =>
                from(this.parameterService.pollParameterContextUpdate(updateRequest.request)).pipe(
                    map((response) =>
                        ParameterActions.pollParameterContextUpdateRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ParameterActions.parameterApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    pollParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.pollParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            filter((response) => response.requestEntity.request.complete),
            switchMap(() => of(ParameterActions.stopPollingParameterContextUpdateRequest()))
        )
    );

    stopPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.stopPollingParameterContextUpdateRequest),
            switchMap(() => of(ParameterActions.deleteParameterContextUpdateRequest()))
        )
    );

    deleteParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.deleteParameterContextUpdateRequest),
            concatLatestFrom(() => this.store.select(selectUpdateRequest)),
            switchMap(([, updateRequest]) => {
                if (updateRequest) {
                    return from(this.parameterService.deleteParameterContextUpdate(updateRequest.request)).pipe(
                        map(() => ParameterActions.editParameterContextComplete()),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ParameterActions.parameterApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            )
                        )
                    );
                } else {
                    return of(ParameterActions.editParameterContextComplete());
                }
            })
        )
    );
}
