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

import { createAction, props } from '@ngrx/store';
import {
    CreateUserGroupRequest,
    CreateUserGroupResponse,
    CreateUserRequest,
    CreateUserResponse,
    DeleteUserGroupRequest,
    DeleteUserRequest,
    EditUserGroupDialogRequest,
    EditUserDialogRequest,
    LoadTenantsSuccess,
    UserAccessPoliciesDialogRequest,
    UpdateUserRequest,
    UpdateUserResponse,
    UpdateUserGroupRequest,
    UpdateUserGroupResponse
} from './index';

const USER_PREFIX = '[User Listing]';

export const resetUsersState = createAction(`${USER_PREFIX} Reset Users State`);

export const loadTenants = createAction(`${USER_PREFIX} Load Tenants`);

export const loadTenantsSuccess = createAction(
    `${USER_PREFIX} Load Tenants Success`,
    props<{ response: LoadTenantsSuccess }>()
);

export const usersApiSnackbarError = createAction(
    `${USER_PREFIX} Users Api Snackbar Error`,
    props<{ error: string }>()
);

export const usersApiBannerError = createAction(`${USER_PREFIX} Users Api Banner Error`, props<{ error: string }>());

export const openCreateTenantDialog = createAction(`${USER_PREFIX} Open Create Tenant Dialog`);

export const createUser = createAction(`${USER_PREFIX} Create User`, props<{ request: CreateUserRequest }>());

export const createUserSuccess = createAction(
    `${USER_PREFIX} Create User Success`,
    props<{
        response: CreateUserResponse;
    }>()
);

export const createUserComplete = createAction(
    `${USER_PREFIX} Create User Complete`,
    props<{
        response: CreateUserResponse;
    }>()
);

export const createUserGroup = createAction(
    `${USER_PREFIX} Create User Group`,
    props<{
        request: CreateUserGroupRequest;
    }>()
);

export const createUserGroupSuccess = createAction(
    `${USER_PREFIX} Create User Group Success`,
    props<{
        response: CreateUserGroupResponse;
    }>()
);

export const updateUser = createAction(`${USER_PREFIX} Update User`, props<{ request: UpdateUserRequest }>());

export const updateUserSuccess = createAction(
    `${USER_PREFIX} Update User Success`,
    props<{
        response: UpdateUserResponse;
    }>()
);

export const updateUserComplete = createAction(`${USER_PREFIX} Update User Complete`);

export const updateUserGroup = createAction(
    `${USER_PREFIX} Update User Group`,
    props<{
        request: UpdateUserGroupRequest;
    }>()
);

export const updateUserGroupSuccess = createAction(
    `${USER_PREFIX} Update User Group Success`,
    props<{
        response: UpdateUserGroupResponse;
    }>()
);

export const selectTenant = createAction(`${USER_PREFIX} Select Tenant`, props<{ id: string }>());

export const navigateToEditTenant = createAction(`${USER_PREFIX} Navigate To Edit Tenant`, props<{ id: string }>());

export const openConfigureUserDialog = createAction(
    `${USER_PREFIX} Open Configure User Dialog`,
    props<{ request: EditUserDialogRequest }>()
);

export const openConfigureUserGroupDialog = createAction(
    `${USER_PREFIX} Open Configure User Group Dialog`,
    props<{ request: EditUserGroupDialogRequest }>()
);

export const navigateToViewAccessPolicies = createAction(
    `${USER_PREFIX} Navigate To View Access Policies`,
    props<{ id: string }>()
);

export const openUserAccessPoliciesDialog = createAction(
    `${USER_PREFIX} Open User Access Policy Dialog`,
    props<{ request: UserAccessPoliciesDialogRequest }>()
);

export const promptDeleteUser = createAction(
    `${USER_PREFIX} Prompt Delete User`,
    props<{ request: DeleteUserRequest }>()
);

export const deleteUser = createAction(`${USER_PREFIX} Delete User`, props<{ request: DeleteUserRequest }>());

export const promptDeleteUserGroup = createAction(
    `${USER_PREFIX} Prompt Delete User Group`,
    props<{ request: DeleteUserGroupRequest }>()
);

export const deleteUserGroup = createAction(
    `${USER_PREFIX} Delete User Group`,
    props<{ request: DeleteUserGroupRequest }>()
);
