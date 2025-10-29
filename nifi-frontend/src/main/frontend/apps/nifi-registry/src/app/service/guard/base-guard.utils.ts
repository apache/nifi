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
import {
    ActivatedRouteSnapshot,
    CanActivateFn,
    CanMatchFn,
    Router,
    RouterStateSnapshot,
    UrlSegment,
    UrlTree
} from '@angular/router';
import { Store } from '@ngrx/store';
import { NiFiRegistryState } from '../../state';
import { RegistryAuthService } from '../registry-auth.service';
import { loadCurrentUser } from '../../state/current-user/current-user.actions';
import { selectCurrentUserState } from '../../state/current-user/current-user.selectors';
import * as ErrorActions from '../../state/error/error.actions';
import { CurrentUser, CurrentUserState } from '../../state/current-user';
import { Observable, filter, map, of, switchMap, take } from 'rxjs';

interface AuthorizationCheck {
    (currentUser: CurrentUser): boolean;
}

const fallbackUrl = '/explorer';

export const computeRequestedUrl = (segments: UrlSegment[]): string => {
    const url = `/${segments
        .map((segment) => segment.path)
        .filter((part) => !!part)
        .join('/')}`;
    return url.length > 1 ? url : '/explorer';
};

export const computeRequestedUrlFromState = (state: RouterStateSnapshot): string => state?.url ?? '/explorer';

export const showAccessDenied = (store: Store, router: Router): UrlTree => {
    store.dispatch(ErrorActions.snackBarError({ error: 'Access denied. Please contact your system administrator.' }));
    return router.parseUrl(fallbackUrl);
};

const redirectToLogin = (
    store: Store,
    router: Router,
    authService: RegistryAuthService,
    requestedUrl: string
): UrlTree => {
    authService.setRedirectUrl(requestedUrl || fallbackUrl);
    return router.parseUrl('/login');
};

const evaluateAuthorization = (
    store: Store<NiFiRegistryState>,
    router: Router,
    authService: RegistryAuthService,
    state: CurrentUserState,
    authorizationCheck: AuthorizationCheck,
    requestedUrl: string
): boolean | UrlTree => {
    const authorized = authorizationCheck(state.currentUser);
    if (authorized) {
        return true;
    }

    if (state.currentUser.anonymous) {
        return redirectToLogin(store, router, authService, requestedUrl);
    }

    return showAccessDenied(store, router);
};

const authorize = (requestedUrl: string, authorizationCheck: AuthorizationCheck): Observable<boolean | UrlTree> => {
    const store = inject(Store<NiFiRegistryState>);
    const router = inject(Router);
    const authService = inject(RegistryAuthService);

    const state$ = store.select(selectCurrentUserState);

    return state$.pipe(
        take(1),
        switchMap((state) => {
            if (state.status === 'success') {
                return of(evaluateAuthorization(store, router, authService, state, authorizationCheck, requestedUrl));
            }

            if (state.status === 'error') {
                return of(redirectToLogin(store, router, authService, requestedUrl));
            }

            authService.setRedirectUrl(requestedUrl);
            if (state.status === 'pending') {
                store.dispatch(loadCurrentUser());
            }

            return state$.pipe(
                filter((nextState) => nextState.status === 'success' || nextState.status === 'error'),
                take(1),
                map((nextState) =>
                    nextState.status === 'success'
                        ? evaluateAuthorization(store, router, authService, nextState, authorizationCheck, requestedUrl)
                        : redirectToLogin(store, router, authService, requestedUrl)
                )
            );
        })
    );
};

export const buildCanMatchGuard = (authorizationCheck: AuthorizationCheck): CanMatchFn => {
    return (_route, segments) => authorize(computeRequestedUrl(segments), authorizationCheck);
};

export const buildCanActivateGuard = (authorizationCheck: AuthorizationCheck): CanActivateFn => {
    return (_route: ActivatedRouteSnapshot, state: RouterStateSnapshot) =>
        authorize(computeRequestedUrlFromState(state), authorizationCheck);
};

export const buildGuard = (authorizationCheck: AuthorizationCheck): CanMatchFn =>
    buildCanMatchGuard(authorizationCheck);
