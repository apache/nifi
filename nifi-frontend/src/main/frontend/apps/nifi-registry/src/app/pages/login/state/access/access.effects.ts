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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as AccessActions from './access.actions';
import { catchError, map, of, switchMap, tap } from 'rxjs';
import { RegistryApiService } from '../../../../service/registry-api.service';
import { RegistryAuthService } from '../../../../service/registry-auth.service';
import { Router } from '@angular/router';
import { HttpErrorResponse } from '@angular/common/http';
import * as ErrorActions from '../../../../state/error/error.actions';
import { Store } from '@ngrx/store';
import { NiFiRegistryState } from '../../../../state';
import { loadCurrentUser } from '../../../../state/current-user/current-user.actions';

@Injectable()
export class AccessEffects {
    private actions$ = inject(Actions);
    private registryApi = inject(RegistryApiService);
    private authService = inject(RegistryAuthService);
    private router = inject(Router);
    private store = inject<Store<NiFiRegistryState>>(Store);

    login$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessActions.login),
            switchMap(({ request }) =>
                this.registryApi.login(request.username, request.password).pipe(
                    map((jwt) => {
                        this.authService.storeToken(jwt);
                        return AccessActions.loginSuccess();
                    }),
                    catchError((error: HttpErrorResponse) =>
                        of(
                            AccessActions.loginFailure({
                                loginFailure:
                                    error?.error?.message || error?.message || 'Unable to login. Please try again.'
                            })
                        )
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
                    const redirect = this.authService.consumeRedirectUrl() || '/explorer';
                    this.router.navigateByUrl(redirect);
                    this.store.dispatch(loadCurrentUser());
                })
            ),
        { dispatch: false }
    );

    loginFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessActions.loginFailure),
            map(({ loginFailure }) =>
                ErrorActions.snackBarError({ error: loginFailure || 'Unable to login. Please try again.' })
            )
        )
    );
}
