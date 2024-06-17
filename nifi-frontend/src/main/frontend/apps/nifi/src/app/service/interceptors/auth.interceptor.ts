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

import { inject } from '@angular/core';
import { HttpErrorResponse, HttpHandlerFn, HttpInterceptorFn, HttpRequest } from '@angular/common/http';
import { catchError, take, combineLatest, tap, NEVER, switchMap } from 'rxjs';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../state';
import { fullScreenError, setRoutedToFullScreenError } from '../../state/error/error.actions';
import { selectCurrentUserState } from '../../state/current-user/current-user.selectors';
import { navigateToLogIn, resetCurrentUser } from '../../state/current-user/current-user.actions';
import { selectRoutedToFullScreenError } from '../../state/error/error.selectors';
import { selectLoginConfiguration } from '../../state/login-configuration/login-configuration.selectors';

export const authInterceptor: HttpInterceptorFn = (request: HttpRequest<unknown>, next: HttpHandlerFn) => {
    const store: Store<NiFiState> = inject(Store<NiFiState>);

    return next(request).pipe(
        catchError((errorResponse) => {
            if (errorResponse instanceof HttpErrorResponse && errorResponse.status === 401) {
                return combineLatest([
                    store.select(selectCurrentUserState).pipe(
                        take(1),
                        tap(() => store.dispatch(resetCurrentUser()))
                    ),
                    store.select(selectLoginConfiguration).pipe(take(1)),
                    store.select(selectRoutedToFullScreenError).pipe(
                        take(1),
                        tap(() => store.dispatch(setRoutedToFullScreenError({ routedToFullScreenError: true })))
                    )
                ]).pipe(
                    switchMap(([currentUserState, loginConfiguration, routedToFullScreenError]) => {
                        if (
                            currentUserState.status === 'pending' &&
                            loginConfiguration?.loginSupported &&
                            !routedToFullScreenError
                        ) {
                            store.dispatch(navigateToLogIn());
                        } else {
                            store.dispatch(
                                fullScreenError({
                                    errorDetail: {
                                        title: 'Unauthorized',
                                        message: 'Your session has expired. Please navigate home to log in again.'
                                    }
                                })
                            );
                        }

                        return NEVER;
                    })
                );
            } else {
                throw errorResponse;
            }
        })
    );
};
