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

import { createReducer, on } from '@ngrx/store';
import { CurrentUserState, ResourcePermission } from './index';
import { loadCurrentUser, loadCurrentUserSuccess, resetCurrentUser } from './current-user.actions';

export const NO_PERMISSIONS: ResourcePermission = {
    canRead: false,
    canWrite: false,
    canDelete: false
};

export const initialState: CurrentUserState = {
    user: {
        identity: '',
        anonymous: true,
        canVersionFlows: false,
        loginSupported: false,
        oidcloginSupported: false,
        resourcePermission: {
            anyTopLevelResource: NO_PERMISSIONS,
            buckets: NO_PERMISSIONS,
            policies: NO_PERMISSIONS,
            proxy: NO_PERMISSIONS,
            tenants: NO_PERMISSIONS
        }
    },
    status: 'pending'
};

export const currentUserReducer = createReducer(
    initialState,
    on(loadCurrentUser, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadCurrentUserSuccess, (state, { response }) => ({
        ...state,
        user: response.user,
        status: 'success' as const
    })),
    on(resetCurrentUser, () => ({
        ...initialState
    }))
);
