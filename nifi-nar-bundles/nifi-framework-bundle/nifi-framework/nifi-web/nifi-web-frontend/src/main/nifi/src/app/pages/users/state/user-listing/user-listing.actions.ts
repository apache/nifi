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

import { createAction, props } from '@ngrx/store';
import {
    DeleteUserGroupRequest,
    DeleteUserRequest,
    EditUserGroupRequest,
    EditUserRequest,
    LoadTenantsSuccess,
    UserAccessPoliciesRequest,
    UserGroupAccessPoliciesRequest
} from './index';

const USER_PREFIX: string = '[User Listing]';

export const resetUsersState = createAction(`${USER_PREFIX} Reset Users State`);

export const loadTenants = createAction(`${USER_PREFIX} Load Tenants`);

export const loadTenantsSuccess = createAction(
    `${USER_PREFIX} Load Tenants Success`,
    props<{ response: LoadTenantsSuccess }>()
);

export const usersApiError = createAction(`${USER_PREFIX} Users Api Error`, props<{ error: string }>());

export const selectTenant = createAction(`${USER_PREFIX} Select Tenant`, props<{ id: string }>());

export const navigateToEditTenant = createAction(`${USER_PREFIX} Navigate To Edit Tenant`, props<{ id: string }>());

export const openConfigureUserDialog = createAction(
    `${USER_PREFIX} Open Configure User Dialog`,
    props<{ request: EditUserRequest }>()
);

export const openConfigureUserGroupDialog = createAction(
    `${USER_PREFIX} Open Configure User Group Dialog`,
    props<{ request: EditUserGroupRequest }>()
);

export const navigateToViewAccessPolicies = createAction(
    `${USER_PREFIX} Navigate To View Access Policies`,
    props<{ id: string }>()
);

export const openUserAccessPoliciesDialog = createAction(
    `${USER_PREFIX} Open User Access Policy Dialog`,
    props<{ request: UserAccessPoliciesRequest }>()
);

export const openUserGroupAccessPoliciesDialog = createAction(
    `${USER_PREFIX} Open User Group Access Policy Dialog`,
    props<{ request: UserGroupAccessPoliciesRequest }>()
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
