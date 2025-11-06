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
import { provideMockActions } from '@ngrx/effects/testing';
import { Observable, ReplaySubject, of, throwError } from 'rxjs';
import { take, toArray } from 'rxjs/operators';
import { CurrentUserEffects } from './current-user.effects';
import { RegistryApiService } from '../../service/registry-api.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { RegistryAuthService } from '../../service/registry-auth.service';
import * as CurrentUserActions from './current-user.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { Store } from '@ngrx/store';
import { resetLoginFailure } from '../../pages/login/state/access/access.actions';
import * as ErrorActions from '../error/error.actions';

describe('CurrentUserEffects', () => {
    let actions$: Observable<unknown>;
    let effects: CurrentUserEffects;
    let authService: jest.Mocked<RegistryAuthService>;
    let errorHelper: jest.Mocked<ErrorHelper>;
    let store: Store<any>;

    beforeEach(() => {
        const registryApiServiceMock = {
            getCurrentUser: jest.fn()
        } as unknown as RegistryApiService;

        authService = {
            ticketExchange: jest.fn(),
            clearToken: jest.fn(),
            getStoredToken: jest.fn(),
            logout: jest.fn()
        } as unknown as jest.Mocked<RegistryAuthService>;

        errorHelper = {
            getErrorString: jest.fn().mockReturnValue('error')
        } as unknown as jest.Mocked<ErrorHelper>;

        store = {
            dispatch: jest.fn(),
            select: jest.fn().mockReturnValue(of(null))
        } as unknown as Store<any>;

        TestBed.configureTestingModule({
            providers: [
                CurrentUserEffects,
                provideMockActions(() => actions$),
                { provide: RegistryApiService, useValue: registryApiServiceMock },
                { provide: RegistryAuthService, useValue: authService },
                { provide: ErrorHelper, useValue: errorHelper },
                { provide: Store, useValue: store }
            ]
        });

        effects = TestBed.inject(CurrentUserEffects);
    });

    describe('logout$', () => {
        it('should emit navigateToLogout when logout succeeds', (done) => {
            authService.logout.mockReturnValue(of(undefined));
            actions$ = of(CurrentUserActions.logout());
            effects.logout$.subscribe((result) => {
                expect(result).toEqual(CurrentUserActions.navigateToLogout());
                expect(authService.logout).toHaveBeenCalled();
                done();
            });
        });

        it('should emit logoutFailure when logout fails', (done) => {
            const error = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            authService.logout.mockReturnValue(throwError(() => error));
            const subject = new ReplaySubject(1);
            subject.next(CurrentUserActions.logout());
            actions$ = subject.asObservable();

            effects.logout$.subscribe((result) => {
                expect(result).toEqual(CurrentUserActions.logoutFailure({ error }));
                expect(authService.logout).toHaveBeenCalled();
                done();
            });
        });
    });

    describe('navigateToLogout$', () => {
        const originalLocation = window.location;

        beforeEach(() => {
            Object.defineProperty(window, 'location', {
                configurable: true,
                value: {
                    origin: 'https://localhost',
                    href: 'https://localhost/old'
                }
            });
        });

        afterEach(() => {
            Object.defineProperty(window, 'location', {
                configurable: true,
                value: originalLocation
            });
        });

        it('should clear token and redirect to backend logout endpoint', (done) => {
            const subject = new ReplaySubject(1);
            subject.next(CurrentUserActions.navigateToLogout());
            actions$ = subject.asObservable();

            effects.navigateToLogout$.subscribe(() => {
                expect(authService.clearToken).toHaveBeenCalled();
                expect(window.location.href).toEqual('https://localhost/nifi-registry/logout');
                done();
            });
        });
    });

    it('should emit snack bar and reset actions when logout fails', (done) => {
        const error = new HttpErrorResponse({ status: 401, statusText: 'Unauthorized' });
        const subject = new ReplaySubject(1);
        subject.next(CurrentUserActions.logoutFailure({ error }));
        actions$ = subject.asObservable();

        effects.logoutFailure$.pipe(take(2), toArray()).subscribe((actions) => {
            expect(actions[0]).toEqual(ErrorActions.snackBarError({ error: 'error' }));
            expect(actions[1]).toEqual(resetLoginFailure());
            done();
        });
    });
});
