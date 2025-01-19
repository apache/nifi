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
import * as GeneralActions from './general.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { ControllerService } from '../../service/controller.service';
import { MatDialog } from '@angular/material/dialog';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { selectStatus } from './general.selectors';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { SMALL_DIALOG } from '@nifi/shared';

@Injectable()
export class GeneralEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private controllerService: ControllerService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog
    ) {}

    loadControllerConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(GeneralActions.loadControllerConfig),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                from(this.controllerService.getControllerConfig()).pipe(
                    map((response) =>
                        GeneralActions.loadControllerConfigSuccess({
                            response: {
                                controller: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                )
            )
        )
    );

    updateControllerConfig$ = createEffect(() =>
        this.actions$.pipe(
            ofType(GeneralActions.updateControllerConfig),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerService.updateControllerConfig(request.controller)).pipe(
                    map((response) =>
                        GeneralActions.updateControllerConfigSuccess({
                            response: {
                                controller: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            GeneralActions.controllerConfigApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    updateControllerConfigSuccess = createEffect(
        () =>
            this.actions$.pipe(
                ofType(GeneralActions.updateControllerConfigSuccess),
                tap(() => {
                    this.dialog.open(OkDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Settings',
                            message: 'Settings successfully applied'
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    controllerConfigApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(GeneralActions.controllerConfigApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );
}
