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
import { Action } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import * as CurrentUserActions from './current-user.actions';
import * as ErrorActions from '../error/error.actions';
import { RegistryApiService } from '../../service/registry-api.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { catchError, map, switchMap, tap } from 'rxjs/operators';
import { Observable, of, from } from 'rxjs';
import { RegistryAuthService } from '../../service/registry-auth.service';
import { CurrentUser } from './index';
import { resetLoginFailure } from '../../pages/login/state/access/access.actions';

@Injectable()
export class CurrentUserEffects {
    private actions$ = inject(Actions);
    private registryApi = inject(RegistryApiService);
    private errorHelper = inject(ErrorHelper);
    private authService = inject(RegistryAuthService);

    loadCurrentUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(CurrentUserActions.loadCurrentUser),
            switchMap(() =>
                this.registryApi.getCurrentUser().pipe(
                    map((currentUser) =>
                        CurrentUserActions.loadCurrentUserSuccess({
                            response: { currentUser: this.normalizeCurrentUser(currentUser) }
                        })
                    ),
                    catchError((error: HttpErrorResponse) => this.handleLoadError(error))
                )
            )
        )
    );

    private handleLoadError(error: HttpErrorResponse): Observable<Action> {
        if (error.status === 401) {
            return this.authService.ticketExchange().pipe(
                switchMap((jwt) => {
                    if (!jwt) {
                        return this.handleFailure(error);
                    }
                    return this.registryApi.getCurrentUser().pipe(
                        map((currentUser) =>
                            CurrentUserActions.loadCurrentUserSuccess({
                                response: { currentUser: this.normalizeCurrentUser(currentUser) }
                            })
                        ),
                        catchError((retryError: HttpErrorResponse) => this.handleFailure(retryError))
                    );
                }),
                catchError((exchangeError: HttpErrorResponse) => this.handleFailure(exchangeError))
            );
        }

        return this.handleFailure(error);
    }

    private handleFailure(error: HttpErrorResponse): Observable<Action> {
        if (error.status === 401) {
            this.authService.clearToken();
        }

        const message = this.errorHelper.getErrorString(error, 'Unable to load current user');
        return of(CurrentUserActions.loadCurrentUserFailure({ error }), ErrorActions.snackBarError({ error: message }));
    }

    logout$ = createEffect(() =>
        this.actions$.pipe(
            ofType(CurrentUserActions.logout),
            switchMap(() =>
                from(this.authService.logout()).pipe(
                    map(() => CurrentUserActions.navigateToLogout()),
                    catchError((error: HttpErrorResponse) => of(CurrentUserActions.logoutFailure({ error })))
                )
            )
        )
    );

    logoutFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(CurrentUserActions.logoutFailure),
            switchMap(({ error }) =>
                of(
                    ErrorActions.snackBarError({
                        error: this.errorHelper.getErrorString(error, 'Unable to logout. Please try again.')
                    }),
                    resetLoginFailure()
                )
            )
        )
    );

    navigateToLogout$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(CurrentUserActions.navigateToLogout),
                tap(() => {
                    this.authService.clearToken();
                    window.location.href = `${window.location.origin}/nifi-registry/logout`;
                })
            ),
        { dispatch: false }
    );

    private normalizeCurrentUser(currentUser: CurrentUser): CurrentUser {
        const hasToken = !!this.authService.getStoredToken();

        return {
            ...currentUser,
            canLogout: !currentUser.anonymous && hasToken,
            canActivateResourcesAuthGuard:
                !currentUser.anonymous ||
                currentUser.resourcePermissions.anyTopLevelResource.canRead ||
                currentUser.resourcePermissions.buckets.canRead ||
                currentUser.resourcePermissions.tenants.canRead ||
                currentUser.resourcePermissions.policies.canRead ||
                currentUser.resourcePermissions.proxy.canRead
        };
    }
}
