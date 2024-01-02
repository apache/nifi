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

import { AccessPolicyState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    addTenantsToPolicy,
    createAccessPolicySuccess,
    accessPolicyApiError,
    loadAccessPolicy,
    loadAccessPolicySuccess,
    loadTenants,
    loadTenantsSuccess,
    removeTenantFromPolicy,
    resetAccessPolicyState,
    resetAccessPolicy,
    setAccessPolicy
} from './access-policy.actions';
import { TenantEntity } from '../../../../state/shared';

export const initialState: AccessPolicyState = {
    users: [],
    userGroups: [],
    saving: false,
    loadedTimestamp: '',
    error: null,
    status: 'pending'
};

export const accessPolicyReducer = createReducer(
    initialState,
    on(setAccessPolicy, (state, { request }) => ({
        ...state,
        resourceAction: request.resourceAction
    })),
    on(loadAccessPolicy, (state, { request }) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadAccessPolicySuccess, createAccessPolicySuccess, (state, { response }) => ({
        ...state,
        accessPolicy: response.accessPolicy,
        policyStatus: response.policyStatus,
        loadedTimestamp: response.accessPolicy.generated,
        saving: false,
        status: 'success' as const
    })),
    on(loadTenants, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadTenantsSuccess, (state, { response }) => ({
        ...state,
        users: response.users,
        userGroups: response.userGroups,
        status: 'success' as const
    })),
    on(addTenantsToPolicy, (state, { request }) => ({
        ...state,
        saving: true
    })),
    on(removeTenantFromPolicy, (state, { request }) => ({
        ...state,
        saving: true
    })),
    on(resetAccessPolicy, (state, { response }) => ({
        ...state,
        accessPolicy: undefined,
        policyStatus: response.policyStatus,
        loadedTimestamp: 'N/A',
        status: 'success' as const
    })),
    on(accessPolicyApiError, (state, { response }) => ({
        ...state,
        error: response.error,
        accessPolicy: undefined,
        policyStatus: undefined,
        status: 'error' as const
    })),
    on(resetAccessPolicyState, (state) => ({
        ...initialState
    }))
);
