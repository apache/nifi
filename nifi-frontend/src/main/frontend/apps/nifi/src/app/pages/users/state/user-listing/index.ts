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

import { AccessPolicySummaryEntity, UserEntity, UserGroupEntity } from '../../../../state/shared';
import { Revision } from '@nifi/shared';

export interface SelectedTenant {
    id: string;
    user?: UserEntity;
    userGroup?: UserGroupEntity;
}

export interface LoadTenantsSuccess {
    users: UserEntity[];
    userGroups: UserGroupEntity[];
    loadedTimestamp: string;
}

export interface CreateUserRequest {
    revision: Revision;
    userPayload: any;
    userGroupUpdate?: {
        requestId: number;
        userGroups: string[];
    };
}

export interface CreateUserResponse {
    user: UserEntity;
    userGroupUpdate?: {
        requestId: number;
        userGroups: string[];
    };
}

export interface CreateUserGroupRequest {
    revision: Revision;
    userGroupPayload: any;
}

export interface CreateUserGroupResponse {
    userGroup: UserGroupEntity;
}

export interface UpdateUserRequest {
    revision: Revision;
    id: string;
    uri: string;
    userPayload: any;
    userGroupUpdate?: {
        requestId: number;
        userGroupsAdded: string[];
        userGroupsRemoved: string[];
    };
}

export interface UpdateUserResponse {
    user: UserEntity;
    userGroupUpdate?: {
        requestId: number;
        userGroupsAdded: string[];
        userGroupsRemoved: string[];
    };
}

export interface UpdateUserGroupRequest {
    requestId?: number;
    revision: Revision;
    id: string;
    uri: string;
    userGroupPayload: any;
}

export interface UpdateUserGroupResponse {
    requestId?: number;
    userGroup: UserGroupEntity;
}

export interface EditUserDialogRequest {
    user: UserEntity;
}

export interface EditUserGroupDialogRequest {
    userGroup: UserGroupEntity;
}

export interface DeleteUserRequest {
    user: UserEntity;
}

export interface DeleteUserGroupRequest {
    userGroup: UserGroupEntity;
}

export interface UserAccessPoliciesDialogRequest {
    id: string;
    identity: string;
    accessPolicies: AccessPolicySummaryEntity[];
}

export interface UserListingState {
    users: UserEntity[];
    userGroups: UserGroupEntity[];
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
