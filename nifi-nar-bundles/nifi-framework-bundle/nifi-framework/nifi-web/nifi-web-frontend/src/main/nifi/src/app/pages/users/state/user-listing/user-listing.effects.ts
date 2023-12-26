/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import * as UserListingActions from './user-listing.actions';
import { catchError, combineLatest, from, map, of, switchMap, take, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { UsersService } from '../../service/users.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';

@Injectable()
export class UserListingEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private router: Router,
        private usersService: UsersService,
        private dialog: MatDialog
    ) {}

    loadTenants$ = createEffect(() =>
        this.actions$.pipe(
            ofType(UserListingActions.loadTenants),
            switchMap(() =>
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
                    catchError((error) =>
                        of(
                            UserListingActions.usersApiError({
                                error: error.error
                            })
                        )
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

    promptDeleteUser$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(UserListingActions.promptDeleteUser),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete User Account',
                            message: `Are you sure you want to delete the user account for '${request.user.component.identity}'?`
                        },
                        panelClass: 'small-dialog'
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
                    map((response) => UserListingActions.loadTenants()),
                    catchError((error) => of(UserListingActions.usersApiError({ error: error.error })))
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
                        data: {
                            title: 'Delete User Account',
                            message: `Are you sure you want to delete the user group account for '${request.userGroup.component.identity}'?`
                        },
                        panelClass: 'small-dialog'
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
                    catchError((error) => of(UserListingActions.usersApiError({ error: error.error })))
                )
            )
        )
    );
}
