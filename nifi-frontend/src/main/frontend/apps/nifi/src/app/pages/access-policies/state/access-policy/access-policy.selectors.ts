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

import { createSelector } from '@ngrx/store';
import { AccessPoliciesState, selectAccessPoliciesState } from '../index';
import { accessPolicyFeatureKey, AccessPolicyState } from './index';
import { selectCurrentRoute } from '@nifi/shared';
import { ComponentResourceAction, ResourceAction } from '../shared';

export const selectAccessPolicyState = createSelector(
    selectAccessPoliciesState,
    (state: AccessPoliciesState) => state[accessPolicyFeatureKey]
);

export const selectResourceAction = createSelector(
    selectAccessPolicyState,
    (state: AccessPolicyState) => state.resourceAction
);

export const selectAccessPolicy = createSelector(
    selectAccessPolicyState,
    (state: AccessPolicyState) => state.accessPolicy
);

export const selectSaving = createSelector(selectAccessPolicyState, (state: AccessPolicyState) => state.saving);

export const selectGlobalResourceActionFromRoute = createSelector(selectCurrentRoute, (route) => {
    let selectedResourceAction: ResourceAction | null = null;
    if (route?.params.action && route?.params.resource) {
        // always select the action and resource from the route
        selectedResourceAction = {
            action: route.params.action,
            resource: route.params.resource,
            resourceIdentifier: route.params.resourceIdentifier
        };
    }
    return selectedResourceAction;
});

export const selectComponentResourceActionFromRoute = createSelector(selectCurrentRoute, (route) => {
    let selectedResourceAction: ComponentResourceAction | null = null;
    if (route?.params.action && route?.params.policy && route?.params.resource && route?.params.resourceIdentifier) {
        // always select the action and resource from the route
        selectedResourceAction = {
            action: route.params.action,
            policy: route.params.policy,
            resource: route.params.resource,
            resourceIdentifier: route.params.resourceIdentifier
        };
    }
    return selectedResourceAction;
});
