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

import { CanActivateFn, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiRegistryState } from '../../state';
import { selectCurrentUserState } from '../../state/current-user/current-user.selectors';
import { RegistryAuthService } from '../registry-auth.service';
import { loadCurrentUser } from '../../state/current-user/current-user.actions';
import { filter, map, switchMap, take } from 'rxjs/operators';
import { Observable, of } from 'rxjs';
import { CurrentUserState } from '../../state/current-user';

const handleLoginState = (
    router: Router,
    authService: RegistryAuthService,
    state: CurrentUserState,
    requestedUrl: string
): boolean | UrlTree => {
    const currentUser = state.currentUser;

    if (!currentUser.anonymous && currentUser.canActivateResourcesAuthGuard) {
        const redirectUrl = authService.consumeRedirectUrl() ?? requestedUrl ?? '/explorer';
        router.navigateByUrl(redirectUrl);
        return false;
    }

    if (currentUser.anonymous && !currentUser.loginSupported && !currentUser.oidcLoginSupported) {
        router.navigateByUrl('/explorer');
        return false;
    }

    return true;
};

export const loginGuard: CanActivateFn = (_route, state: RouterStateSnapshot): Observable<boolean | UrlTree> => {
    const store = inject(Store<NiFiRegistryState>);
    const router = inject(Router);
    const authService = inject(RegistryAuthService);

    const requestedUrl = state?.url ?? '/explorer';

    return store.select(selectCurrentUserState).pipe(
        take(1),
        switchMap((currentState) => {
            if (currentState.status === 'success') {
                return of(handleLoginState(router, authService, currentState, requestedUrl));
            }

            if (currentState.status === 'error') {
                return of(true);
            }

            if (currentState.status === 'pending') {
                store.dispatch(loadCurrentUser());
            }

            return store.select(selectCurrentUserState).pipe(
                filter((nextState) => nextState.status === 'success' || nextState.status === 'error'),
                take(1),
                map((nextState) =>
                    nextState.status === 'success'
                        ? handleLoginState(router, authService, nextState, requestedUrl)
                        : true
                )
            );
        })
    );
};
