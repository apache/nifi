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
import { CurrentUserState, currentUserFeatureKey } from './index';

export const selectCurrentUserState = createFeatureSelector<CurrentUserState>(currentUserFeatureKey);

export const selectCurrentUser = createSelector(selectCurrentUserState, (state) => state.currentUser);

export const selectCurrentUserStatus = createSelector(selectCurrentUserState, (state) => state.status);

export const selectCurrentUserError = createSelector(selectCurrentUserState, (state) => state.error);

export const selectAnyTopLevelRead = createSelector(
    selectCurrentUser,
    (currentUser) => currentUser.resourcePermissions.anyTopLevelResource.canRead
);

export const selectBucketsRead = createSelector(
    selectCurrentUser,
    (currentUser) => currentUser.resourcePermissions.buckets.canRead
);

export const selectTenantsRead = createSelector(
    selectCurrentUser,
    (currentUser) => currentUser.resourcePermissions.tenants.canRead
);

export const selectLogoutSupported = createSelector(selectCurrentUser, (currentUser) => currentUser.canLogout);
