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

import { Component, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { selectUser } from '../../../../state/user/user.selectors';
import { UserEntity, UserGroupEntity, UserListingState } from '../../state/user-listing';
import {
    selectSelectedTenant,
    selectSingleEditedTenant,
    selectTenantForAccessPolicies,
    selectTenantIdFromRoute,
    selectUserListingState
} from '../../state/user-listing/user-listing.selectors';
import { initialState } from '../../state/user-listing/user-listing.reducer';
import {
    loadTenants,
    navigateToEditTenant,
    navigateToViewAccessPolicies,
    openConfigureUserDialog,
    openConfigureUserGroupDialog,
    openUserAccessPoliciesDialog,
    openUserGroupAccessPoliciesDialog,
    promptDeleteUser,
    promptDeleteUserGroup,
    selectTenant
} from '../../state/user-listing/user-listing.actions';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'user-listing',
    templateUrl: './user-listing.component.html',
    styleUrls: ['./user-listing.component.scss']
})
export class UserListing implements OnInit {
    userListingState$ = this.store.select(selectUserListingState);
    selectedTenantId$ = this.store.select(selectTenantIdFromRoute);
    currentUser$ = this.store.select(selectUser);

    constructor(private store: Store<UserListingState>) {
        this.store
            .select(selectSingleEditedTenant)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectSelectedTenant(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((selectedTenant) => {
                if (selectedTenant?.user) {
                    this.store.dispatch(
                        openConfigureUserDialog({
                            request: {
                                user: selectedTenant.user
                            }
                        })
                    );
                } else if (selectedTenant?.userGroup) {
                    this.store.dispatch(
                        openConfigureUserGroupDialog({
                            request: {
                                userGroup: selectedTenant.userGroup
                            }
                        })
                    );
                }
            });

        this.store
            .select(selectTenantForAccessPolicies)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectSelectedTenant(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((selectedTenant) => {
                if (selectedTenant?.user) {
                    this.store.dispatch(
                        openUserAccessPoliciesDialog({
                            request: {
                                user: selectedTenant.user
                            }
                        })
                    );
                } else if (selectedTenant?.userGroup) {
                    this.store.dispatch(
                        openUserGroupAccessPoliciesDialog({
                            request: {
                                userGroup: selectedTenant.userGroup
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadTenants());
    }

    isInitialLoading(state: UserListingState): boolean {
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    selectTenant(id: string): void {
        this.store.dispatch(
            selectTenant({
                id
            })
        );
    }

    editTenant(id: string): void {
        this.store.dispatch(
            navigateToEditTenant({
                id
            })
        );
    }

    deleteUser(user: UserEntity): void {
        this.store.dispatch(
            promptDeleteUser({
                request: {
                    user
                }
            })
        );
    }

    deleteUserGroup(userGroup: UserGroupEntity): void {
        this.store.dispatch(
            promptDeleteUserGroup({
                request: {
                    userGroup
                }
            })
        );
    }

    viewAccessPolicies(id: string): void {
        this.store.dispatch(
            navigateToViewAccessPolicies({
                id
            })
        );
    }

    refreshUserListing() {
        this.store.dispatch(loadTenants());
    }
}
