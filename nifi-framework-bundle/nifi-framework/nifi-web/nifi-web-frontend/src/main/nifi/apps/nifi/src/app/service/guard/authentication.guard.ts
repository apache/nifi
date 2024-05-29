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
import { AuthService } from '../auth.service';
import { catchError, from, map, of, switchMap, take, tap } from 'rxjs';
import { CurrentUserService } from '../current-user.service';
import { Store } from '@ngrx/store';
import { CurrentUserState } from '../../state/current-user';
import { loadCurrentUserSuccess } from '../../state/current-user/current-user.actions';
import { selectCurrentUserState } from '../../state/current-user/current-user.selectors';
import { HttpErrorResponse } from '@angular/common/http';
import { fullScreenError } from '../../state/error/error.actions';
import { ErrorHelper } from '../error-helper.service';
import { selectLoginConfiguration } from '../../state/login-configuration/login-configuration.selectors';
import { loadLoginConfigurationSuccess } from '../../state/login-configuration/login-configuration.actions';

export const authenticationGuard: CanMatchFn = () => {
    const authService: AuthService = inject(AuthService);
    const userService: CurrentUserService = inject(CurrentUserService);
    const errorHelper: ErrorHelper = inject(ErrorHelper);
    const store: Store<CurrentUserState> = inject(Store<CurrentUserState>);

    const getAuthenticationConfig = store.select(selectLoginConfiguration).pipe(
        take(1),
        switchMap((loginConfiguration) => {
            if (loginConfiguration) {
                return of(loginConfiguration);
            } else {
                return from(authService.getLoginConfiguration()).pipe(
                    tap((response) => {
                        store.dispatch(
                            loadLoginConfigurationSuccess({
                                response: {
                                    loginConfiguration: response.authenticationConfiguration
                                }
                            })
                        );
                    })
                );
            }
        })
    );

    return getAuthenticationConfig.pipe(
        switchMap((authConfigResponse) => {
            return store.select(selectCurrentUserState).pipe(
                take(1),
                switchMap((userState) => {
                    if (userState.status == 'success') {
                        return of(true);
                    } else {
                        return from(userService.getUser()).pipe(
                            tap((response) => {
                                store.dispatch(
                                    loadCurrentUserSuccess({
                                        response: {
                                            user: response
                                        }
                                    })
                                );
                            }),
                            map(() => true),
                            catchError((errorResponse: HttpErrorResponse) => {
                                if (errorResponse.status !== 401 || authConfigResponse.loginSupported) {
                                    store.dispatch(errorHelper.fullScreenError(errorResponse));
                                }

                                return of(false);
                            })
                        );
                    }
                })
            );
        }),
        catchError(() => {
            store.dispatch(
                fullScreenError({
                    errorDetail: {
                        title: 'Unauthorized',
                        message:
                            'Unable to load authentication configuration. Please contact your system administrator.'
                    }
                })
            );

            return of(false);
        })
    );
};
