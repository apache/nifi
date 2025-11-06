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
import {
    loadPolicies,
    loadPoliciesFailure,
    loadPoliciesSuccess,
    loadPolicyTenants,
    loadPolicyTenantsFailure,
    loadPolicyTenantsSuccess,
    saveBucketPolicy,
    saveBucketPolicyFailure
} from './policies.actions';
import { PoliciesState } from '.';
import { produce } from 'immer';

export const initialState: PoliciesState = {
    bucketId: null,
    tenants: {
        users: [],
        userGroups: []
    },
    loadingPolicies: false,
    loadingTenants: false,
    saving: false,
    error: null,
    policySelection: {
        read: undefined,
        write: undefined,
        delete: undefined
    }
};

export const policiesReducer = createReducer(
    initialState,
    on(loadPolicies, (state) =>
        produce(state, (draft) => {
            draft.loadingPolicies = true;
            draft.error = null;
        })
    ),
    on(loadPoliciesSuccess, (state, { response }) =>
        produce(state, (draft) => {
            draft.bucketId = response.bucketId;
            draft.loadingPolicies = false;
            draft.saving = false; // Also clear saving state when policies reload

            // Filter policies to only those matching this bucket
            const bucketResource = `/buckets/${response.bucketId}`;
            const bucketPolicies = response.policies.filter((policy) => policy.resource === bucketResource);

            // Extract read, write, delete policies
            const readPolicy = bucketPolicies.find((p) => p.action === 'read');
            const writePolicy = bucketPolicies.find((p) => p.action === 'write');
            const deletePolicy = bucketPolicies.find((p) => p.action === 'delete');

            draft.policySelection = {
                read: readPolicy
                    ? {
                          policyId: readPolicy.identifier,
                          revision: readPolicy.revision,
                          users: readPolicy.users,
                          userGroups: readPolicy.userGroups
                      }
                    : undefined,
                write: writePolicy
                    ? {
                          policyId: writePolicy.identifier,
                          revision: writePolicy.revision,
                          users: writePolicy.users,
                          userGroups: writePolicy.userGroups
                      }
                    : undefined,
                delete: deletePolicy
                    ? {
                          policyId: deletePolicy.identifier,
                          revision: deletePolicy.revision,
                          users: deletePolicy.users,
                          userGroups: deletePolicy.userGroups
                      }
                    : undefined
            };
        })
    ),
    on(loadPoliciesFailure, (state) =>
        produce(state, (draft) => {
            draft.loadingPolicies = false;
        })
    ),
    on(loadPolicyTenants, (state) =>
        produce(state, (draft) => {
            draft.loadingTenants = true;
        })
    ),
    on(loadPolicyTenantsSuccess, (state, { response }) =>
        produce(state, (draft) => {
            draft.tenants = response;
            draft.loadingTenants = false;
        })
    ),
    on(loadPolicyTenantsFailure, (state) =>
        produce(state, (draft) => {
            draft.loadingTenants = false;
        })
    ),
    on(saveBucketPolicy, (state) =>
        produce(state, (draft) => {
            draft.saving = true;
        })
    ),
    on(saveBucketPolicyFailure, (state) =>
        produce(state, (draft) => {
            draft.saving = false;
        })
    )
);
