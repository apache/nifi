/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestBed } from '@angular/core/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { ReplaySubject, of, take, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';

import * as UserListingActions from './user-listing.actions';
import { UserListingEffects } from './user-listing.effects';
import { UsersService } from '../../service/users.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { Client } from '../../../../service/client.service';
import { NiFiCommon } from '@nifi/shared';
import { initialState } from './user-listing.reducer';

describe('UserListingEffects', () => {
    interface SetupOptions {
        userListingState?: any;
    }

    const mockUsersResponse = { users: [], generated: '2023-01-01 12:00:00 EST' };
    const mockUserGroupsResponse = { userGroups: [] };

    async function setup({ userListingState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                UserListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        users: {
                            users: userListingState
                        }
                    }
                }),
                { provide: UsersService, useValue: { getUsers: jest.fn(), getUserGroups: jest.fn() } },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn() } },
                { provide: MatDialog, useValue: { open: jest.fn(), closeAll: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                { provide: Client, useValue: {} },
                { provide: NiFiCommon, useValue: {} }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(UserListingEffects);
        const usersService = TestBed.inject(UsersService);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, store, usersService, errorHelper };
    }

    let action$: ReplaySubject<Action>;
    beforeEach(() => {
        action$ = new ReplaySubject<Action>();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Tenants', () => {
        it('should load tenants successfully', async () => {
            const { effects, usersService } = await setup();

            action$.next(UserListingActions.loadTenants());
            jest.spyOn(usersService, 'getUsers').mockReturnValueOnce(of(mockUsersResponse) as never);
            jest.spyOn(usersService, 'getUserGroups').mockReturnValueOnce(of(mockUserGroupsResponse) as never);

            const result = await new Promise((resolve) => effects.loadTenants$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                UserListingActions.loadTenantsSuccess({
                    response: {
                        users: mockUsersResponse.users,
                        userGroups: mockUserGroupsResponse.userGroups,
                        loadedTimestamp: mockUsersResponse.generated
                    }
                })
            );
        });

        it('should fail to load tenants on initial load (no existing data)', async () => {
            const { effects, usersService } = await setup();

            action$.next(UserListingActions.loadTenants());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(usersService, 'getUsers').mockImplementationOnce(() => throwError(() => error));
            jest.spyOn(usersService, 'getUserGroups').mockReturnValueOnce(of(mockUserGroupsResponse) as never);

            const result = await new Promise((resolve) => effects.loadTenants$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                UserListingActions.loadTenantsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load tenants on refresh (existing data)', async () => {
            const stateWithData = { ...initialState, loadedTimestamp: '2023-01-01 11:00:00 EST' };
            const { effects, usersService } = await setup({ userListingState: stateWithData });

            action$.next(UserListingActions.loadTenants());
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(usersService, 'getUsers').mockImplementationOnce(() => throwError(() => error));
            jest.spyOn(usersService, 'getUserGroups').mockReturnValueOnce(of(mockUserGroupsResponse) as never);

            const result = await new Promise((resolve) => effects.loadTenants$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                UserListingActions.loadTenantsError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Load Tenants Error', () => {
        it('should handle loadTenantsError with pending status', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = UserListingActions.usersApiBannerError({ error: 'Error loading' });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            action$.next(
                UserListingActions.loadTenantsError({
                    errorResponse: error,
                    loadedTimestamp: '',
                    status: 'pending'
                })
            );

            const result = await new Promise((resolve) => effects.loadTenantsError$.pipe(take(1)).subscribe(resolve));

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle loadTenantsError with success status', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = UserListingActions.usersApiBannerError({ error: 'Error loading' });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            action$.next(
                UserListingActions.loadTenantsError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const result = await new Promise((resolve) => effects.loadTenantsError$.pipe(take(1)).subscribe(resolve));

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
