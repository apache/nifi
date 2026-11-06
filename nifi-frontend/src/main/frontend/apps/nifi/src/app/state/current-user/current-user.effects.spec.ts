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

import { TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { provideMockActions } from '@ngrx/effects/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { SessionStorageService } from '@nifi/shared';
import { firstValueFrom, Observable, of } from 'rxjs';
import { CurrentUserEffects } from './current-user.effects';
import { loadCurrentUserSuccess, navigateToLogIn } from './current-user.actions';
import { CurrentUserService } from '../../service/current-user.service';
import { AuthService } from '../../service/auth.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { RETURN_URL } from '../index';
import { initialState as currentUserInitialState } from './current-user.reducer';

describe('CurrentUserEffects', () => {
    const loadSuccess = loadCurrentUserSuccess({
        response: {
            user: currentUserInitialState.user
        }
    });

    async function setup(action: Action) {
        const actions$: Observable<Action> = of(action);
        const router = { navigate: vi.fn(), navigateByUrl: vi.fn() };

        await TestBed.configureTestingModule({
            providers: [
                CurrentUserEffects,
                provideMockActions(() => actions$),
                provideMockStore({ initialState: {} }),
                { provide: Router, useValue: router },
                { provide: CurrentUserService, useValue: {} },
                { provide: AuthService, useValue: {} },
                { provide: ErrorHelper, useValue: {} }
            ]
        }).compileComponents();

        return {
            effects: TestBed.inject(CurrentUserEffects),
            sessionStorage: TestBed.inject(SessionStorageService),
            router
        };
    }

    beforeEach(() => {
        window.sessionStorage.clear();
        window.location.hash = '';
    });

    afterEach(() => {
        window.sessionStorage.clear();
        window.location.hash = '';
    });

    describe('navigateToLogIn$', () => {
        it('should store the requested route, including its query string, and navigate to login', async () => {
            window.location.hash = '#/process-groups/abc?param=1';
            const { effects, sessionStorage, router } = await setup(navigateToLogIn());

            await firstValueFrom(effects.navigateToLogIn$);

            expect(sessionStorage.getItem<string>(RETURN_URL)).toBe('/process-groups/abc?param=1');
            expect(router.navigate).toHaveBeenCalledWith(['/login']);
        });

        it.each(['/login', '/logout-complete', '/error', '/404'])('should not store %s', async (route) => {
            window.location.hash = `#${route}`;
            const { effects, sessionStorage, router } = await setup(navigateToLogIn());

            await firstValueFrom(effects.navigateToLogIn$);

            expect(sessionStorage.getItem<string>(RETURN_URL)).toBeNull();
            expect(router.navigate).toHaveBeenCalledWith(['/login']);
        });

        it.each(['/login?returnTo=/settings', '/error?message=x', '/404?x=1'])(
            'should not store %s ignoring the query string',
            async (route) => {
                window.location.hash = `#${route}`;
                const { effects, sessionStorage } = await setup(navigateToLogIn());

                await firstValueFrom(effects.navigateToLogIn$);

                expect(sessionStorage.getItem<string>(RETURN_URL)).toBeNull();
            }
        );

        it('should not store an empty hash', async () => {
            const { effects, sessionStorage, router } = await setup(navigateToLogIn());

            await firstValueFrom(effects.navigateToLogIn$);

            expect(sessionStorage.getItem<string>(RETURN_URL)).toBeNull();
            expect(router.navigate).toHaveBeenCalledWith(['/login']);
        });
    });

    describe('restoreReturnUrl$', () => {
        it('should navigate to the stored url and remove it', async () => {
            const { effects, sessionStorage, router } = await setup(loadSuccess);
            sessionStorage.setItem<string>(RETURN_URL, '/settings?tab=1');

            await firstValueFrom(effects.restoreReturnUrl$);

            expect(router.navigateByUrl).toHaveBeenCalledWith('/settings?tab=1');
            expect(sessionStorage.getItem<string>(RETURN_URL)).toBeNull();
        });

        it('should not navigate when no return url is stored', async () => {
            const { effects, router } = await setup(loadSuccess);

            await firstValueFrom(effects.restoreReturnUrl$);

            expect(router.navigateByUrl).not.toHaveBeenCalled();
        });
    });
});
