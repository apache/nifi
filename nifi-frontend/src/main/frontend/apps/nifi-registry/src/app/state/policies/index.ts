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

import { PolicySubject } from '../../service/buckets.service';

export const policiesFeatureKey = 'policies';

export type PolicySection = 'read' | 'write' | 'delete';

export interface PolicySelection {
    policyId?: string;
    revision?: { version: number; clientId?: string };
    users: PolicySubject[];
    userGroups: PolicySubject[];
}

export interface BucketPolicyOptionsView {
    groups: { key: string; label: string; type: 'group'; subject: PolicySubject }[];
    users: { key: string; label: string; type: 'user'; subject: PolicySubject }[];
    all: { key: string; label: string; type: 'user' | 'group'; subject: PolicySubject }[];
    lookup: Record<string, PolicySubject>;
}

export interface PoliciesState {
    bucketId: string | null;
    tenants: {
        users: PolicySubject[];
        userGroups: PolicySubject[];
    };
    loadingPolicies: boolean;
    loadingTenants: boolean;
    saving: boolean;
    error: string | null;
    policySelection: Partial<Record<PolicySection, PolicySelection>>;
}
