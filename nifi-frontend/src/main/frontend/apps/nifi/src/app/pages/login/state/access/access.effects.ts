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
import * as AccessActions from './access.actions';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { AuthService } from '../../../../service/auth.service';
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { MEDIUM_DIALOG } from '@nifi/shared';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { resetLoginFailure } from './access.actions';

@Injectable()
export class AccessEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private authService: AuthService,
        private router: Router,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper
    ) {}

    login$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessActions.login),
            map((action) => action.request),
            switchMap((request) =>
                from(this.authService.login(request.username, request.password)).pipe(
                    map(() => AccessActions.loginSuccess()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(AccessActions.loginFailure({ loginFailure: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    loginSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessActions.loginSuccess),
                tap(() => {
                    this.router.navigate(['/']);
                })
            ),
        { dispatch: false }
    );

    loginFailure$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessActions.loginFailure),
                map((action) => action.loginFailure),
                tap((loginFailure) => {
                    this.dialog
                        .open(OkDialog, {
                            ...MEDIUM_DIALOG,
                            data: {
                                title: 'Login',
                                message: loginFailure
                            }
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(resetLoginFailure());
                        });
                })
            ),
        { dispatch: false }
    );
}
