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

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import * as UserListingActions from './user-listing.actions';
import { selectTenant } from './user-listing.actions';
import { catchError, combineLatest, filter, from, map, mergeMap, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { UsersService } from '../../service/users.service';
import { YesNoDialog } from '@nifi/shared';
import { EditTenantDialog } from '../../../../ui/common/edit-tenant/edit-tenant-dialog.component';
import { selectSaving, selectStatus, selectUserGroups, selectUsers } from './user-listing.selectors';
import { EditTenantRequest, UserGroupEntity } from '../../../../state/shared';
import { Client } from '../../../../service/client.service';
import { LARGE_DIALOG, MEDIUM_DIALOG, SMALL_DIALOG, NiFiCommon } from '@nifi/shared';
import { UserAccessPolicies } from '../../ui/user-listing/user-access-policies/user-access-policies.component';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class UserListingEffects {
    private requestId = 0;

    constructor(
        private actions$: Actions,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private store: Store<NiFiState>,
        private router: Router,
        private usersService: UsersService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog
    ) {}

    loadTenants$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.loadTenants),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                combineLatest([this.usersService.getUsers(), this.usersService.getUserGroups()]).pipe(
                    map(([usersResponse, userGroupsResponse]) =>
                        UserListingActions.loadTenantsSuccess({
                            response: {
                                users: usersResponse.users,
                                userGroups: userGroupsResponse.userGroups,
                                loadedTimestamp: usersResponse.generated
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                )
            )
        )
    );

    selectTenant$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.selectTenant),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/users', id]);
                })
            ),
        { dispatch: false }
    );

    openCreateTenantDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.openCreateTenantDialog),
                concatLatestFrom(() => [this.store.select(selectUsers), this.store.select(selectUserGroups)]),
                tap(([, existingUsers, existingUserGroups]) => {
                    const editTenantRequest: EditTenantRequest = {
                        existingUsers,
                        existingUserGroups
                    };
                    const dialogReference = this.dialog.open(EditTenantDialog, {
                        ...MEDIUM_DIALOG,
                        data: editTenantRequest
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.editTenant
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((response) => {
                            if (response.user) {
                                this.store.dispatch(
                                    UserListingActions.createUser({
                                        request: {
                                            revision: response.revision,
                                            userPayload: response.user.payload,
                                            userGroupUpdate: {
                                                requestId: this.requestId++,
                                                userGroups: response.user.userGroupsAdded
                                            }
                                        }
                                    })
                                );
                            } else if (response.userGroup) {
                                const users: any[] = response.userGroup.users.map((id: string) => {
                                    return { id };
                                });

                                this.store.dispatch(
                                    UserListingActions.createUserGroup({
                                        request: {
                                            revision: response.revision,
                                            userGroupPayload: {
                                                ...response.userGroup.payload,
                                                users
                                            }
                                        }
                                    })
                                );
                            }
                        });
                })
            ),
        { dispatch: false }
    );

    createUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.createUser),
            map((action) => action.request),
            switchMap((request) =>
                from(this.usersService.createUser(request)).pipe(
                    map((response) =>
                        UserListingActions.createUserSuccess({
                            response: {
                                user: response,
                                userGroupUpdate: request.userGroupUpdate
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            UserListingActions.usersApiSnackbarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                )
            )
        )
    );

    createUserSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.createUserSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectUserGroups)),
            switchMap(([response, userGroups]) => {
                if (response.userGroupUpdate) {
                    const userGroupUpdate = response.userGroupUpdate;
                    const userGroupUpdates = [];

                    if (!this.nifiCommon.isEmpty(userGroupUpdate.userGroups)) {
                        userGroupUpdates.push(
                            ...userGroupUpdate.userGroups
                                .map((userGroupId: string) =>
                                    userGroups.find((userGroup) => userGroup.id == userGroupId)
                                )
                                .filter((userGroup) => userGroup != null)
                                .map((userGroup) => {
                                    // @ts-ignore
                                    const ug: UserGroupEntity = userGroup;

                                    const users: any[] = [
                                        ...ug.component.users.map((user) => {
                                            return {
                                                id: user.id
                                            };
                                        }),
                                        { id: response.user.id }
                                    ];

                                    return UserListingActions.updateUserGroup({
                                        request: {
                                            requestId: userGroupUpdate.requestId,
                                            id: ug.id,
                                            uri: ug.uri,
                                            revision: this.client.getRevision(userGroup),
                                            userGroupPayload: {
                                                ...ug.component,
                                                users
                                            }
                                        }
                                    });
                                })
                        );
                    }

                    if (userGroupUpdates.length === 0) {
                        return of(UserListingActions.createUserComplete({ response }));
                    } else {
                        return userGroupUpdates;
                    }
                } else {
                    return of(UserListingActions.createUserComplete({ response }));
                }
            })
        )
    );

    usersApiBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.usersApiBannerError),
            map((action) => action.error),
            switchMap((error) =>
                of(ErrorActions.addBannerError({ errorContext: { errors: [error], context: ErrorContextKey.USERS } }))
            )
        )
    );

    usersApiSnackbarError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.usersApiSnackbarError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    awaitUpdateUserGroupsForCreateUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.createUserSuccess),
            map((action) => action.response),
            filter((response) => response.userGroupUpdate != null),
            mergeMap((createUserResponse) =>
                this.actions$.pipe(
                    ofType(UserListingActions.updateUserGroupSuccess),
                    filter(
                        (updateSuccess) =>
                            // @ts-ignore
                            createUserResponse.userGroupUpdate.requestId === updateSuccess.response.requestId
                    ),
                    map(() => UserListingActions.createUserComplete({ response: createUserResponse }))
                )
            )
        )
    );

    createUserComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.createUserComplete),
            map((action) => action.response),
            tap((response) => {
                this.dialog.closeAll();
                this.store.dispatch(selectTenant({ id: response.user.id }));
            }),
            switchMap(() => of(UserListingActions.loadTenants()))
        )
    );

    createUserGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.createUserGroup),
            map((action) => action.request),
            switchMap((request) =>
                from(this.usersService.createUserGroup(request)).pipe(
                    map((response) =>
                        UserListingActions.createUserGroupSuccess({
                            response: {
                                userGroup: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            UserListingActions.usersApiSnackbarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        );
                    })
                )
            )
        )
    );

    createUserGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.createUserGroupSuccess),
            map((action) => action.response),
            tap((response) => {
                this.dialog.closeAll();
                this.store.dispatch(selectTenant({ id: response.userGroup.id }));
            }),
            switchMap(() => of(UserListingActions.loadTenants()))
        )
    );

    navigateToEditTenant$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.navigateToEditTenant),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/users', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureUserDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.openConfigureUserDialog),
                map((action) => action.request),
                concatLatestFrom(() => [this.store.select(selectUsers), this.store.select(selectUserGroups)]),
                tap(([request, existingUsers, existingUserGroups]) => {
                    const editTenantRequest: EditTenantRequest = {
                        user: request.user,
                        existingUsers,
                        existingUserGroups
                    };
                    const dialogReference = this.dialog.open(EditTenantDialog, {
                        ...MEDIUM_DIALOG,
                        data: editTenantRequest
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.editTenant
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((response) => {
                            if (response.user) {
                                const userGroupsAdded: string[] = response.user.userGroupsAdded;
                                const userGroupsRemoved: string[] = response.user.userGroupsRemoved;

                                if (
                                    this.nifiCommon.isEmpty(userGroupsAdded) &&
                                    this.nifiCommon.isEmpty(userGroupsRemoved)
                                ) {
                                    this.store.dispatch(
                                        UserListingActions.updateUser({
                                            request: {
                                                revision: response.revision,
                                                id: request.user.id,
                                                uri: request.user.uri,
                                                userPayload: {
                                                    ...request.user.component,
                                                    ...response.user.payload
                                                }
                                            }
                                        })
                                    );
                                } else {
                                    this.store.dispatch(
                                        UserListingActions.updateUser({
                                            request: {
                                                revision: response.revision,
                                                id: request.user.id,
                                                uri: request.user.uri,
                                                userPayload: {
                                                    ...request.user.component,
                                                    ...response.user.payload
                                                },
                                                userGroupUpdate: {
                                                    requestId: this.requestId++,
                                                    userGroupsAdded: response.user.userGroupsAdded,
                                                    userGroupsRemoved: response.user.userGroupsRemoved
                                                }
                                            }
                                        })
                                    );
                                }
                            }
                        });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(
                            selectTenant({
                                id: request.user.id
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    updateUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.updateUser),
            map((action) => action.request),
            switchMap((request) =>
                from(this.usersService.updateUser(request)).pipe(
                    map((response) =>
                        UserListingActions.updateUserSuccess({
                            response: {
                                user: response,
                                userGroupUpdate: request.userGroupUpdate
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            UserListingActions.usersApiBannerError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    updateUserSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.updateUserSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectUserGroups)),
            switchMap(([response, userGroups]) => {
                if (response.userGroupUpdate) {
                    const userGroupUpdate = response.userGroupUpdate;
                    const userGroupUpdates = [];

                    if (!this.nifiCommon.isEmpty(userGroupUpdate.userGroupsAdded)) {
                        userGroupUpdates.push(
                            ...userGroupUpdate.userGroupsAdded
                                .map((userGroupId: string) =>
                                    userGroups.find((userGroup) => userGroup.id == userGroupId)
                                )
                                .filter((userGroup) => userGroup != null)
                                .map((userGroup) => {
                                    // @ts-ignore
                                    const ug: UserGroupEntity = userGroup;

                                    const users: any[] = [
                                        ...ug.component.users.map((user) => {
                                            return {
                                                id: user.id
                                            };
                                        }),
                                        { id: response.user.id }
                                    ];

                                    return UserListingActions.updateUserGroup({
                                        request: {
                                            requestId: userGroupUpdate.requestId,
                                            revision: this.client.getRevision(userGroup),
                                            id: ug.id,
                                            uri: ug.uri,
                                            userGroupPayload: {
                                                ...ug.component,
                                                users
                                            }
                                        }
                                    });
                                })
                        );
                    }

                    if (!this.nifiCommon.isEmpty(userGroupUpdate.userGroupsRemoved)) {
                        userGroupUpdates.push(
                            ...userGroupUpdate.userGroupsRemoved
                                .map((userGroupId: string) =>
                                    userGroups.find((userGroup) => userGroup.id == userGroupId)
                                )
                                .filter((userGroup) => userGroup != null)
                                .map((userGroup) => {
                                    // @ts-ignore
                                    const ug: UserGroupEntity = userGroup;

                                    const users: any[] = [
                                        ...ug.component.users
                                            .filter((user) => user.id != response.user.id)
                                            .map((user) => {
                                                return {
                                                    id: user.id
                                                };
                                            })
                                    ];

                                    return UserListingActions.updateUserGroup({
                                        request: {
                                            requestId: userGroupUpdate.requestId,
                                            revision: this.client.getRevision(userGroup),
                                            id: ug.id,
                                            uri: ug.uri,
                                            userGroupPayload: {
                                                ...ug.component,
                                                users
                                            }
                                        }
                                    });
                                })
                        );
                    }

                    if (userGroupUpdates.length === 0) {
                        return of(UserListingActions.updateUserComplete());
                    } else {
                        return userGroupUpdates;
                    }
                } else {
                    return of(UserListingActions.updateUserComplete());
                }
            })
        )
    );

    awaitUpdateUserGroupsForUpdateUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.updateUserSuccess),
            map((action) => action.response),
            filter((response) => response.userGroupUpdate != null),
            mergeMap((updateUserResponse) =>
                this.actions$.pipe(
                    ofType(UserListingActions.updateUserGroupSuccess),
                    filter(
                        (updateSuccess) =>
                            // @ts-ignore
                            updateUserResponse.userGroupUpdate.requestId === updateSuccess.response.requestId
                    ),
                    map(() => UserListingActions.updateUserComplete())
                )
            )
        )
    );

    updateUserComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.updateUserComplete),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap(() => of(UserListingActions.loadTenants()))
        )
    );

    openConfigureUserGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.openConfigureUserGroupDialog),
                map((action) => action.request),
                concatLatestFrom(() => [this.store.select(selectUsers), this.store.select(selectUserGroups)]),
                tap(([request, existingUsers, existingUserGroups]) => {
                    const editTenantRequest: EditTenantRequest = {
                        userGroup: request.userGroup,
                        existingUsers,
                        existingUserGroups
                    };
                    const dialogReference = this.dialog.open(EditTenantDialog, {
                        ...MEDIUM_DIALOG,
                        data: editTenantRequest
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.editTenant
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((response) => {
                            if (response.userGroup) {
                                const users: any[] = response.userGroup.users.map((id: string) => {
                                    return { id };
                                });

                                this.store.dispatch(
                                    UserListingActions.updateUserGroup({
                                        request: {
                                            revision: response.revision,
                                            id: response.userGroup.id,
                                            uri: request.userGroup.uri,
                                            userGroupPayload: {
                                                ...request.userGroup.component,
                                                ...response.userGroup.payload,
                                                users
                                            }
                                        }
                                    })
                                );
                            }
                        });

                    dialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(
                            selectTenant({
                                id: request.userGroup.id
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    updateUserGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.updateUserGroup),
            map((action) => action.request),
            switchMap((request) =>
                from(this.usersService.updateUserGroup(request)).pipe(
                    map((response) =>
                        UserListingActions.updateUserGroupSuccess({
                            response: {
                                requestId: request.requestId,
                                userGroup: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            UserListingActions.usersApiBannerError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    updateUserGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.updateUserGroupSuccess),
            map((action) => action.response),
            filter((response) => response.requestId == null),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap(() => of(UserListingActions.loadTenants()))
        )
    );

    navigateToViewAccessPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.navigateToViewAccessPolicies),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/users', id, 'policies']);
                })
            ),
        { dispatch: false }
    );

    openUserAccessPoliciesDialog = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.openUserAccessPoliciesDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(UserAccessPolicies, {
                            ...LARGE_DIALOG,
                            data: request,
                            autoFocus: false
                        })
                        .afterClosed()
                        .subscribe((response) => {
                            if (response != 'ROUTED') {
                                this.store.dispatch(
                                    selectTenant({
                                        id: request.id
                                    })
                                );
                            }
                        });
                })
            ),
        { dispatch: false }
    );

    promptDeleteUser$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.promptDeleteUser),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete User Account',
                            message: `Are you sure you want to delete the user account for '${request.user.component.identity}'?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            UserListingActions.deleteUser({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteUser$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.deleteUser),
            map((action) => action.request),
            switchMap((request) =>
                from(this.usersService.deleteUser(request.user)).pipe(
                    map(() => UserListingActions.loadTenants()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            UserListingActions.usersApiSnackbarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    promptDeleteUserGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.promptDeleteUserGroup),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete User Account',
                            message: `Are you sure you want to delete the user group account for '${request.userGroup.component.identity}'?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            UserListingActions.deleteUserGroup({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteUserGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.deleteUserGroup),
            map((action) => action.request),
            switchMap((request) =>
                from(this.usersService.deleteUserGroup(request.userGroup)).pipe(
                    map(() => UserListingActions.loadTenants()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            UserListingActions.usersApiSnackbarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );
}
