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
import { selectUpdateAttributeState, UpdateAttributeState } from '../index';
import { AdvancedUiParameters, advancedUiParametersFeatureKey, AdvancedUiParametersState } from './index';
import { selectCurrentRoute } from '@nifi/shared';

export const selectAdvancedUiParametersState = createSelector(
    selectUpdateAttributeState,
    (state: UpdateAttributeState) => state[advancedUiParametersFeatureKey]
);

export const selectAdvancedUiParameters = createSelector(
    selectAdvancedUiParametersState,
    (state: AdvancedUiParametersState) => state.advancedUiParameters
);

export const selectAdvancedUiParametersFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        return {
            clientId: route.queryParams.clientId,
            revision: Number(route.queryParams.revision) || 0,
            disconnectedNodeAcknowledged: route.queryParams.disconnectedNodeAcknowledged === 'true',
            editable: route.queryParams.editable === 'true',
            processorId: route.queryParams.id
        } as AdvancedUiParameters;
    }
    return null;
});

export const selectEditable = createSelector(
    selectAdvancedUiParameters,
    (state: AdvancedUiParameters) => state.editable
);
