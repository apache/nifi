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

import { HttpErrorResponse } from '@angular/common/http';

export const currentUserFeatureKey = 'currentUser';

export interface LoadCurrentUserResponse {
    currentUser: CurrentUser;
}

export interface ResourcePermissionDetails {
    canRead: boolean;
    canWrite: boolean;
    canDelete: boolean;
}

export interface ResourcePermissions {
    anyTopLevelResource: ResourcePermissionDetails;
    buckets: ResourcePermissionDetails;
    tenants: ResourcePermissionDetails;
    policies: ResourcePermissionDetails;
    proxy: ResourcePermissionDetails;
}

export interface CurrentUser {
    identity: string;
    anonymous: boolean;
    loginSupported: boolean;
    oidcLoginSupported: boolean;
    canLogout?: boolean;
    canActivateResourcesAuthGuard?: boolean;
    resourcePermissions: ResourcePermissions;
}

export interface CurrentUserState {
    currentUser: CurrentUser;
    status: 'pending' | 'loading' | 'success' | 'error';
    error: HttpErrorResponse | null;
}
