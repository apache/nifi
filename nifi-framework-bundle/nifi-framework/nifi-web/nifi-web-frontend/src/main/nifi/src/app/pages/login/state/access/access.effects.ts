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
import { catchError, combineLatest, from, map, of, switchMap, tap } from 'rxjs';
import { AuthService } from '../../../../service/auth.service';
import { AuthStorage } from '../../../../service/auth-storage.service';
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { MEDIUM_DIALOG } from '../../../../index';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';

@Injectable()
export class AccessEffects {
    constructor(
        private actions$: Actions,
        private authService: AuthService,
        private authStorage: AuthStorage,
        private router: Router,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper
    ) {}

    loadAccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessActions.loadAccess),
            switchMap(() =>
                combineLatest([this.authService.accessConfig(), this.authService.accessStatus()]).pipe(
                    map(([accessConfig, accessStatus]) =>
                        AccessActions.loadAccessSuccess({
                            response: {
                                accessConfig: accessConfig.config,
                                accessStatus: accessStatus.accessStatus
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessActions.accessApiError({
                                error: {
                                    title: 'Unable to check Access Status',
                                    message: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    login$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessActions.login),
            map((action) => action.request),
            switchMap((request) =>
                from(this.authService.login(request.username, request.password)).pipe(
                    map((jwt) => {
                        const sessionExpiration: string | null = this.authService.getSessionExpiration(jwt);
                        if (sessionExpiration) {
                            this.authStorage.setToken(sessionExpiration);
                        }
                        return AccessActions.verifyAccess();
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(AccessActions.loginFailure({ failure: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );

    loginFailure$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessActions.loginFailure),
                map((action) => action.failure),
                tap((failure) => {
                    this.dialog.open(OkDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            title: 'Login',
                            message: failure
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    verifyAccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessActions.verifyAccess),
            switchMap(() =>
                from(this.authService.accessStatus()).pipe(
                    map((response) => {
                        if (response.accessStatus.status === 'ACTIVE') {
                            return AccessActions.verifyAccessSuccess();
                        } else {
                            return AccessActions.accessApiError({
                                error: {
                                    title: 'Unable to log in',
                                    message: response.accessStatus.message
                                }
                            });
                        }
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessActions.accessApiError({
                                error: {
                                    title: 'Unable to log in',
                                    message: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    verifyAccessSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessActions.verifyAccessSuccess),
                tap(() => {
                    this.router.navigate(['/']);
                })
            ),
        { dispatch: false }
    );
}
