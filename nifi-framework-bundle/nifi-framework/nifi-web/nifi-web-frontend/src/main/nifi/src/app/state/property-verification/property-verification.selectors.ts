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

import { createFeatureSelector, createSelector } from '@ngrx/store';
import { propertyVerificationFeatureKey, PropertyVerificationState } from './index';

export const selectPropertyVerificationState =
    createFeatureSelector<PropertyVerificationState>(propertyVerificationFeatureKey);

export const selectPropertyVerificationRequestContext = createSelector(
    selectPropertyVerificationState,
    (state: PropertyVerificationState) => state.requestContext
);

export const selectActivePropertyVerificationRequest = createSelector(
    selectPropertyVerificationState,
    (state: PropertyVerificationState) => state.activeRequest
);

export const selectPropertyVerificationResults = createSelector(
    selectPropertyVerificationState,
    (state: PropertyVerificationState) => state.results
);

export const selectPropertyVerificationStatus = createSelector(
    selectPropertyVerificationState,
    (state: PropertyVerificationState) => state.status
);

export const selectPropertyVerificationAttributes = createSelector(
    selectPropertyVerificationState,
    (state: PropertyVerificationState) => state.attributes
);
