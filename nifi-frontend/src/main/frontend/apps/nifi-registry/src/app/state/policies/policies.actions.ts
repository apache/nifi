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

import { createAction, props } from '@ngrx/store';
import { PolicySection } from '.';
import { Policy, PolicyRevision, PolicySubject } from '../../service/buckets.service';
import { ErrorContextKey } from '../error';

export const loadPolicies = createAction(
    '[Policies] Load Policies',
    props<{ request: { bucketId: string; context: ErrorContextKey } }>()
);

export const loadPoliciesSuccess = createAction(
    '[Policies] Load Policies Success',
    props<{ response: { bucketId: string; policies: Policy[] } }>()
);

export const loadPoliciesFailure = createAction('[Policies] Load Policies Failure');

export const loadPolicyTenants = createAction(
    '[Policies] Load Policy Tenants',
    props<{ request: { context: ErrorContextKey } }>()
);

export const loadPolicyTenantsSuccess = createAction(
    '[Policies] Load Policy Tenants Success',
    props<{ response: { users: PolicySubject[]; userGroups: PolicySubject[] } }>()
);

export const loadPolicyTenantsFailure = createAction('[Policies] Load Policy Tenants Failure');

export const saveBucketPolicy = createAction(
    '[Policies] Save Bucket Policy',
    props<{
        request: {
            bucketId: string;
            action: PolicySection;
            policyId?: string;
            users: PolicySubject[];
            userGroups: PolicySubject[];
            revision?: PolicyRevision;
            isLastInBatch?: boolean;
        };
    }>()
);

export const saveBucketPolicyFailure = createAction('[Policies] Save Bucket Policy Failure');

export const policyChangeSuccessToast = createAction(
    '[Policies] Policy Change Success Toast',
    props<{ message: string }>()
);

export const policiesNoOp = createAction('[Policies] No Op');
