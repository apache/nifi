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
import { ReplaySubject, of, take, throwError, Subject } from 'rxjs';
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
                {
                    provide: UsersService,
                    useValue: {
                        getUsers: jest.fn(),
                        getUserGroups: jest.fn(),
                        updateUserGroup: jest.fn(),
                        updateUser: jest.fn()
                    }
                },
                { provide: ErrorHelper, useValue: { handleLoadingError: jest.fn(), getErrorString: jest.fn() } },
                { provide: MatDialog, useValue: { open: jest.fn(), closeAll: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                { provide: Client, useValue: { getRevision: jest.fn() } },
                { provide: NiFiCommon, useValue: { isEmpty: jest.fn() } }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const effects = TestBed.inject(UserListingEffects);
        const usersService = TestBed.inject(UsersService);
        const errorHelper = TestBed.inject(ErrorHelper);
        const client = TestBed.inject(Client);
        const nifiCommon = TestBed.inject(NiFiCommon);
        const dialog = TestBed.inject(MatDialog);

        return { effects, store, usersService, errorHelper, client, nifiCommon, dialog };
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

    describe('Update User Group', () => {
        it('should update user group successfully', async () => {
            const { effects, usersService } = await setup();

            const mockResponse = { id: 'ug1', component: { identity: 'group1', users: [] } } as any;
            jest.spyOn(usersService, 'updateUserGroup').mockReturnValueOnce(of(mockResponse) as never);

            const request = {
                requestId: 42,
                revision: { version: 0 },
                id: 'ug1',
                userGroupPayload: { identity: 'group1', users: [{ id: 'u1' }] }
            };

            action$.next(UserListingActions.updateUserGroup({ request }));

            const result = await new Promise((resolve) => effects.updateUserGroup$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                UserListingActions.updateUserGroupSuccess({
                    response: {
                        requestId: 42,
                        userGroup: mockResponse
                    }
                })
            );
        });

        it('should handle update user group error', async () => {
            const { effects, usersService, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(usersService, 'updateUserGroup').mockReturnValueOnce(throwError(() => error) as never);
            jest.spyOn(errorHelper, 'getErrorString').mockReturnValueOnce('Update failed');

            const request = {
                revision: { version: 0 },
                id: 'ug1',
                userGroupPayload: { identity: 'group1', users: [] }
            };

            action$.next(UserListingActions.updateUserGroup({ request }));

            const result = await new Promise((resolve) => effects.updateUserGroup$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(UserListingActions.usersApiBannerError({ error: 'Update failed' }));
        });

        it('should handle multiple concurrent updateUserGroup actions', async () => {
            const { effects, usersService } = await setup();

            const subject1 = new Subject<any>();
            const subject2 = new Subject<any>();
            const subject3 = new Subject<any>();

            const mockUpdateUserGroup = jest.spyOn(usersService, 'updateUserGroup');
            mockUpdateUserGroup
                .mockReturnValueOnce(subject1.asObservable() as never)
                .mockReturnValueOnce(subject2.asObservable() as never)
                .mockReturnValueOnce(subject3.asObservable() as never);

            const results: any[] = [];
            effects.updateUserGroup$.subscribe((result) => results.push(result));

            action$.next(
                UserListingActions.updateUserGroup({
                    request: { requestId: 1, revision: { version: 0 }, id: 'ug1', userGroupPayload: {} }
                })
            );
            action$.next(
                UserListingActions.updateUserGroup({
                    request: { requestId: 1, revision: { version: 0 }, id: 'ug2', userGroupPayload: {} }
                })
            );
            action$.next(
                UserListingActions.updateUserGroup({
                    request: { requestId: 1, revision: { version: 0 }, id: 'ug3', userGroupPayload: {} }
                })
            );

            expect(mockUpdateUserGroup).toHaveBeenCalledTimes(3);

            const mockResponse2 = { id: 'ug2', component: { identity: 'group2', users: [] } } as any;
            subject2.next(mockResponse2);
            subject2.complete();

            expect(results.length).toBe(1);
            expect(results[0]).toEqual(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 1, userGroup: mockResponse2 }
                })
            );

            const mockResponse3 = { id: 'ug3', component: { identity: 'group3', users: [] } } as any;
            subject3.next(mockResponse3);
            subject3.complete();

            expect(results.length).toBe(2);
            expect(results[1]).toEqual(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 1, userGroup: mockResponse3 }
                })
            );

            const mockResponse1 = { id: 'ug1', component: { identity: 'group1', users: [] } } as any;
            subject1.next(mockResponse1);
            subject1.complete();

            expect(results.length).toBe(3);
            expect(results[2]).toEqual(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 1, userGroup: mockResponse1 }
                })
            );
        });
    });

    describe('Update User Success', () => {
        it('should dispatch updateUserComplete when no userGroupUpdate', async () => {
            const { effects } = await setup();

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any
                    }
                })
            );

            const result = await new Promise((resolve) => effects.updateUserSuccess$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(UserListingActions.updateUserComplete());
        });

        it('should dispatch updateUserComplete when userGroupUpdate has empty added and removed', async () => {
            const { effects, nifiCommon } = await setup();

            jest.spyOn(nifiCommon, 'isEmpty').mockReturnValue(true);

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any,
                        userGroupUpdate: {
                            requestId: 1,
                            userGroupsAdded: [],
                            userGroupsRemoved: []
                        }
                    }
                })
            );

            const result = await new Promise((resolve) => effects.updateUserSuccess$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(UserListingActions.updateUserComplete());
        });

        it('should dispatch updateUserGroup actions for added user groups', async () => {
            const userGroupEntity = {
                id: 'ug1',
                revision: { version: 1 },
                component: {
                    identity: 'group1',
                    users: [{ id: 'existingUser' }]
                }
            };

            const stateWithGroups = {
                ...initialState,
                userGroups: [userGroupEntity]
            };

            const { effects, nifiCommon, client } = await setup({ userListingState: stateWithGroups });

            jest.spyOn(nifiCommon, 'isEmpty').mockImplementation((arr: any) => !arr || arr.length === 0);
            jest.spyOn(client, 'getRevision').mockReturnValue({ version: 1 });

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any,
                        userGroupUpdate: {
                            requestId: 5,
                            userGroupsAdded: ['ug1'],
                            userGroupsRemoved: []
                        }
                    }
                })
            );

            const result = await new Promise((resolve) => effects.updateUserSuccess$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                UserListingActions.updateUserGroup({
                    request: {
                        requestId: 5,
                        revision: { version: 1 },
                        id: 'ug1',
                        userGroupPayload: {
                            ...userGroupEntity.component,
                            users: [{ id: 'existingUser' }, { id: 'u1' }]
                        }
                    }
                })
            );
        });

        it('should dispatch updateUserGroup actions for removed user groups', async () => {
            const userGroupEntity = {
                id: 'ug2',
                revision: { version: 2 },
                component: {
                    identity: 'group2',
                    users: [{ id: 'u1' }, { id: 'u2' }]
                }
            };

            const stateWithGroups = {
                ...initialState,
                userGroups: [userGroupEntity]
            };

            const { effects, nifiCommon, client } = await setup({ userListingState: stateWithGroups });

            jest.spyOn(nifiCommon, 'isEmpty').mockImplementation((arr: any) => !arr || arr.length === 0);
            jest.spyOn(client, 'getRevision').mockReturnValue({ version: 2 });

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any,
                        userGroupUpdate: {
                            requestId: 6,
                            userGroupsAdded: [],
                            userGroupsRemoved: ['ug2']
                        }
                    }
                })
            );

            const result = await new Promise((resolve) => effects.updateUserSuccess$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                UserListingActions.updateUserGroup({
                    request: {
                        requestId: 6,
                        revision: { version: 2 },
                        id: 'ug2',
                        userGroupPayload: {
                            ...userGroupEntity.component,
                            users: [{ id: 'u2' }]
                        }
                    }
                })
            );
        });
    });

    describe('Await Update User Groups For CreateUser', () => {
        it('should dispatch createUserComplete when expectedCount is 0', async () => {
            const { effects } = await setup();

            const createUserResponse = {
                user: { id: 'u1' } as any,
                userGroupUpdate: {
                    requestId: 10,
                    userGroups: []
                }
            };

            action$.next(UserListingActions.createUserSuccess({ response: createUserResponse }));

            const result = await new Promise((resolve) =>
                effects.awaitUpdateUserGroupsForCreateUser$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(UserListingActions.createUserComplete({ response: createUserResponse }));
        });

        it('should wait for all updateUserGroupSuccess actions and then dispatch createUserComplete', async () => {
            const { effects } = await setup();

            const createUserResponse = {
                user: { id: 'u1' } as any,
                userGroupUpdate: {
                    requestId: 11,
                    userGroups: ['ug1', 'ug2']
                }
            };

            action$.next(UserListingActions.createUserSuccess({ response: createUserResponse }));

            const results: any[] = [];
            effects.awaitUpdateUserGroupsForCreateUser$.subscribe((result) => results.push(result));

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 11, userGroup: { id: 'ug1' } as any }
                })
            );

            expect(results.length).toBe(1);
            expect(results[0]).toEqual(UserListingActions.createUserComplete({ response: createUserResponse }));

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 11, userGroup: { id: 'ug2' } as any }
                })
            );

            expect(results.length).toBe(2);
            expect(results[1]).toEqual(UserListingActions.createUserComplete({ response: createUserResponse }));
        });

        it('should ignore updateUserGroupSuccess actions with non-matching requestId', async () => {
            const { effects } = await setup();

            const createUserResponse = {
                user: { id: 'u1' } as any,
                userGroupUpdate: {
                    requestId: 12,
                    userGroups: ['ug1']
                }
            };

            action$.next(UserListingActions.createUserSuccess({ response: createUserResponse }));

            const results: any[] = [];
            effects.awaitUpdateUserGroupsForCreateUser$.subscribe((result) => results.push(result));

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 999, userGroup: { id: 'ug1' } as any }
                })
            );

            expect(results.length).toBe(0);

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 12, userGroup: { id: 'ug1' } as any }
                })
            );

            expect(results.length).toBe(1);
            expect(results[0]).toEqual(UserListingActions.createUserComplete({ response: createUserResponse }));
        });

        it('should not emit for createUserSuccess without userGroupUpdate', async () => {
            const { effects } = await setup();

            const createUserResponse = {
                user: { id: 'u1' } as any
            };

            action$.next(UserListingActions.createUserSuccess({ response: createUserResponse }));

            const results: any[] = [];
            effects.awaitUpdateUserGroupsForCreateUser$.subscribe((result) => results.push(result));

            await new Promise((resolve) => setTimeout(resolve, 0));

            expect(results.length).toBe(0);
        });
    });

    describe('Await Update User Groups For Update User', () => {
        it('should dispatch updateUserComplete when expectedCount is 0', async () => {
            const { effects } = await setup();

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any,
                        userGroupUpdate: {
                            requestId: 20,
                            userGroupsAdded: [],
                            userGroupsRemoved: []
                        }
                    }
                })
            );

            const result = await new Promise((resolve) =>
                effects.awaitUpdateUserGroupsForUpdateUser$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(UserListingActions.updateUserComplete());
        });

        it('should wait for all updateUserGroupSuccess actions for added and removed groups', async () => {
            const { effects } = await setup();

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any,
                        userGroupUpdate: {
                            requestId: 21,
                            userGroupsAdded: ['ug1'],
                            userGroupsRemoved: ['ug2']
                        }
                    }
                })
            );

            const results: any[] = [];
            effects.awaitUpdateUserGroupsForUpdateUser$.subscribe((result) => results.push(result));

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 21, userGroup: { id: 'ug1' } as any }
                })
            );

            expect(results.length).toBe(1);
            expect(results[0]).toEqual(UserListingActions.updateUserComplete());

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 21, userGroup: { id: 'ug2' } as any }
                })
            );

            expect(results.length).toBe(2);
            expect(results[1]).toEqual(UserListingActions.updateUserComplete());
        });

        it('should ignore updateUserGroupSuccess actions with non-matching requestId', async () => {
            const { effects } = await setup();

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any,
                        userGroupUpdate: {
                            requestId: 22,
                            userGroupsAdded: ['ug1'],
                            userGroupsRemoved: []
                        }
                    }
                })
            );

            const results: any[] = [];
            effects.awaitUpdateUserGroupsForUpdateUser$.subscribe((result) => results.push(result));

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 999, userGroup: { id: 'ug1' } as any }
                })
            );

            expect(results.length).toBe(0);

            action$.next(
                UserListingActions.updateUserGroupSuccess({
                    response: { requestId: 22, userGroup: { id: 'ug1' } as any }
                })
            );

            expect(results.length).toBe(1);
            expect(results[0]).toEqual(UserListingActions.updateUserComplete());
        });

        it('should not emit for updateUserSuccess without userGroupUpdate', async () => {
            const { effects } = await setup();

            action$.next(
                UserListingActions.updateUserSuccess({
                    response: {
                        user: { id: 'u1' } as any
                    }
                })
            );

            const results: any[] = [];
            effects.awaitUpdateUserGroupsForUpdateUser$.subscribe((result) => results.push(result));

            await new Promise((resolve) => setTimeout(resolve, 0));

            expect(results.length).toBe(0);
        });
    });
});
