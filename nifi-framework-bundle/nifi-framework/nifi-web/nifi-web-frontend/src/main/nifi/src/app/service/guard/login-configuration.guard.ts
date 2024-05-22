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

import { CanMatchFn } from '@angular/router';
import { inject } from '@angular/core';
import { catchError, map, of, switchMap, tap } from 'rxjs';
import { Store } from '@ngrx/store';
import { fullScreenError } from '../../state/error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../error-helper.service';
import { selectLoginConfiguration } from '../../state/login-configuration/login-configuration.selectors';
import { AuthService } from '../auth.service';
import { loadLoginConfigurationSuccess } from '../../state/login-configuration/login-configuration.actions';
import { LoginConfiguration, LoginConfigurationState } from '../../state/login-configuration';

export const checkLoginConfiguration = (
    loginConfigurationCheck: (loginConfiguration: LoginConfiguration) => boolean
): CanMatchFn => {
    return () => {
        const store: Store<LoginConfigurationState> = inject(Store<LoginConfigurationState>);
        const authService: AuthService = inject(AuthService);
        const errorHelper: ErrorHelper = inject(ErrorHelper);

        return store.select(selectLoginConfiguration).pipe(
            switchMap((loginConfiguration) => {
                if (loginConfiguration) {
                    return of(loginConfiguration);
                } else {
                    return authService.getLoginConfiguration().pipe(
                        tap((response) =>
                            store.dispatch(
                                loadLoginConfigurationSuccess({
                                    response: {
                                        loginConfiguration: response.authenticationConfiguration
                                    }
                                })
                            )
                        )
                    );
                }
            }),
            map((loginConfiguration) => {
                if (loginConfigurationCheck(loginConfiguration)) {
                    return true;
                }

                store.dispatch(
                    fullScreenError({
                        skipReplaceUrl: true,
                        errorDetail: {
                            title: 'Unable to load',
                            message: 'Login configuration check failed'
                        }
                    })
                );
                return false;
            }),
            catchError((errorResponse: HttpErrorResponse) => {
                store.dispatch(errorHelper.fullScreenError(errorResponse, true));
                return of(false);
            })
        );
    };
};
