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

import { createFeatureSelector, createSelector } from '@ngrx/store';
import { policiesFeatureKey, BucketPolicyOptionsView, PoliciesState } from './index';
import { PolicySubject } from '../../service/buckets.service';
import { resourcesFeatureKey, ResourcesState } from '..';

export const selectResourcesState = createFeatureSelector<ResourcesState>(resourcesFeatureKey);

export const selectPoliciesState = createSelector(selectResourcesState, (state) => state[policiesFeatureKey]);

export const selectPolicyTenants = createSelector(selectPoliciesState, (state: PoliciesState) => state.tenants);

export const selectPolicyOptions = createSelector(
    selectPolicyTenants,
    ({ users, userGroups }): BucketPolicyOptionsView => {
        const sortedGroups = [...userGroups].sort((a, b) => a.identity.localeCompare(b.identity));
        const sortedUsers = [...users].sort((a, b) => a.identity.localeCompare(b.identity));

        const groups = sortedGroups.map((group) => ({
            key: `group-${group.identifier}`,
            label: group.identity,
            type: 'group' as const,
            subject: group
        }));

        const userOptions = sortedUsers.map((user) => ({
            key: `user-${user.identifier}`,
            label: user.identity,
            type: 'user' as const,
            subject: user
        }));

        const lookup: Record<string, PolicySubject> = {};
        [...groups, ...userOptions].forEach((option) => {
            lookup[option.subject.identifier] = option.subject;
        });

        return {
            groups,
            users: userOptions,
            all: [...groups, ...userOptions],
            lookup
        };
    }
);

export const selectPolicySelection = createSelector(
    selectPoliciesState,
    (state: PoliciesState) => state.policySelection
);

export const selectPoliciesLoading = createSelector(
    selectPoliciesState,
    (state: PoliciesState) => state.loadingPolicies || state.loadingTenants
);

export const selectPoliciesSaving = createSelector(selectPoliciesState, (state: PoliciesState) => state.saving);
