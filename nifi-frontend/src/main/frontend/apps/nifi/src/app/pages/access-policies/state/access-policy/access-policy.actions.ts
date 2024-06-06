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
    LoadAccessPolicyError,
    LoadAccessPolicyRequest,
    AccessPolicyResponse,
    SelectGlobalAccessPolicyRequest,
    SetAccessPolicyRequest,
    ResetAccessPolicy,
    RemoveTenantFromPolicyRequest,
    AddTenantsToPolicyRequest,
    SelectComponentAccessPolicyRequest
} from './index';

const ACCESS_POLICY_PREFIX = '[Access Policy]';

export const selectGlobalAccessPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Select Global Access Policy`,
    props<{ request: SelectGlobalAccessPolicyRequest }>()
);

export const selectComponentAccessPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Select Component Access Policy`,
    props<{ request: SelectComponentAccessPolicyRequest }>()
);

export const setAccessPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Set Access Policy`,
    props<{ request: SetAccessPolicyRequest }>()
);

export const reloadAccessPolicy = createAction(`${ACCESS_POLICY_PREFIX} Reload Access Policy`);

export const loadAccessPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Load Access Policy`,
    props<{ request: LoadAccessPolicyRequest }>()
);

export const loadAccessPolicySuccess = createAction(
    `${ACCESS_POLICY_PREFIX} Load Access Policy Success`,
    props<{ response: AccessPolicyResponse }>()
);

export const createAccessPolicy = createAction(`${ACCESS_POLICY_PREFIX} Create Access Policy`);

export const promptOverrideAccessPolicy = createAction(`${ACCESS_POLICY_PREFIX} Prompt Override Access Policy`);

export const overrideAccessPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Override Access Policy`,
    props<{ request: AddTenantsToPolicyRequest }>()
);

export const createAccessPolicySuccess = createAction(
    `${ACCESS_POLICY_PREFIX} Create Access Policy Success`,
    props<{ response: AccessPolicyResponse }>()
);

export const openAddTenantToPolicyDialog = createAction(`${ACCESS_POLICY_PREFIX} Open Add Tenant To Policy Dialog`);

export const addTenantsToPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Add Tenants To Policy`,
    props<{ request: AddTenantsToPolicyRequest }>()
);

export const promptRemoveTenantFromPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Prompt Remove Tenant From Policy`,
    props<{ request: RemoveTenantFromPolicyRequest }>()
);

export const removeTenantFromPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Remove Tenant From Policy`,
    props<{ request: RemoveTenantFromPolicyRequest }>()
);

export const promptDeleteAccessPolicy = createAction(`${ACCESS_POLICY_PREFIX} Prompt Delete Access Policy`);

export const deleteAccessPolicy = createAction(`${ACCESS_POLICY_PREFIX} Delete Access Policy`);

export const resetAccessPolicy = createAction(
    `${ACCESS_POLICY_PREFIX} Reset Access Policy`,
    props<{ response: ResetAccessPolicy }>()
);

export const accessPolicyApiBannerError = createAction(
    `${ACCESS_POLICY_PREFIX} Access Policy Api Banner Error`,
    props<{ response: LoadAccessPolicyError }>()
);

export const resetAccessPolicyState = createAction(`${ACCESS_POLICY_PREFIX} Reset Access Policy State`);
