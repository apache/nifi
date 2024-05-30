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
import * as LoginConfigurationActions from './login-configuration.actions';
import { catchError, from, map, of, switchMap } from 'rxjs';
import * as ErrorActions from '../error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../service/error-helper.service';
import { AuthService } from '../../service/auth.service';

@Injectable()
export class LoginConfigurationEffects {
    constructor(
        private actions$: Actions,
        private authService: AuthService,
        private errorHelper: ErrorHelper
    ) {}

    loadLoginConfiguration$ = createEffect(() =>
        this.actions$.pipe(
            ofType(LoginConfigurationActions.loadLoginConfiguration),
            switchMap(() => {
                return from(
                    this.authService.getLoginConfiguration().pipe(
                        map((response) =>
                            LoginConfigurationActions.loadLoginConfigurationSuccess({
                                response
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ErrorActions.snackBarError({
                                    error: this.errorHelper.getErrorString(
                                        errorResponse,
                                        'Failed to load Login Configuration.'
                                    )
                                })
                            )
                        )
                    )
                );
            })
        )
    );
}
