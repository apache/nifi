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

import { AccessPolicyEntity, ComponentResourceAction, PolicyStatus, ResourceAction } from '../shared';
import { TenantEntity } from '../../../../state/shared';

export const accessPolicyFeatureKey = 'accessPolicy';

export interface SetAccessPolicyRequest {
    resourceAction: ResourceAction;
}

export interface SelectGlobalAccessPolicyRequest {
    resourceAction: ResourceAction;
}

export interface SelectComponentAccessPolicyRequest {
    resourceAction: ComponentResourceAction;
}

export interface LoadAccessPolicyRequest {
    resourceAction: ResourceAction;
}

export interface AccessPolicyResponse {
    accessPolicy: AccessPolicyEntity;
    policyStatus?: PolicyStatus;
}

export interface ResetAccessPolicy {
    policyStatus: PolicyStatus;
}

export interface LoadAccessPolicyError {
    error: string;
}

export interface RemoveTenantFromPolicyRequest {
    tenantType: 'user' | 'userGroup';
    tenant: TenantEntity;
}

export interface AddTenantToPolicyDialogRequest {
    accessPolicy: AccessPolicyEntity;
}

export interface AddTenantsToPolicyRequest {
    users: TenantEntity[];
    userGroups: TenantEntity[];
}

export interface AccessPolicyState {
    resourceAction?: ResourceAction;
    policyStatus?: PolicyStatus;
    accessPolicy?: AccessPolicyEntity;
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'error' | 'success';
}
