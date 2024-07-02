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

import { createSelector } from '@ngrx/store';
import { usersFeatureKey, UsersState, selectUserState } from '../index';
import { SelectedTenant, UserListingState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectUserListingState = createSelector(selectUserState, (state: UsersState) => state[usersFeatureKey]);

export const selectSaving = createSelector(selectUserListingState, (state: UserListingState) => state.saving);

export const selectStatus = createSelector(selectUserListingState, (state: UserListingState) => state.status);

export const selectUsers = createSelector(selectUserListingState, (state: UserListingState) => state.users);

export const selectUserGroups = createSelector(selectUserListingState, (state: UserListingState) => state.userGroups);

export const selectTenantIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return route.params.id;
    }
    return null;
});

export const selectSingleEditedTenant = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        return route.params.id;
    }
    return null;
});

export const selectTenantForAccessPolicies = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'policies') {
        return route.params.id;
    }
    return null;
});

export const selectSelectedTenant = (id: string) =>
    createSelector(selectUserListingState, (state: UserListingState) => {
        const user = state.users.find((user) => id == user.id);
        if (user) {
            return {
                id,
                user
            } as SelectedTenant;
        }
        const userGroup = state.userGroups.find((userGroup) => id == userGroup.id);
        if (userGroup) {
            return {
                id,
                userGroup
            } as SelectedTenant;
        }
        return null;
    });
