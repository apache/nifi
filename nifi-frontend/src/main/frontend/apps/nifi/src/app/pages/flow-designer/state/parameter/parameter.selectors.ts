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

import { createSelector } from '@ngrx/store';
import { CanvasState, selectCanvasState } from '../index';
import { parameterFeatureKey, ParameterState } from './index';

export const selectParameterState = createSelector(
    selectCanvasState,
    (state: CanvasState) => state[parameterFeatureKey]
);

export const selectUpdateRequest = createSelector(
    selectParameterState,
    (state: ParameterState) => state.updateRequestEntity
);

export const selectParameterSaving = createSelector(selectParameterState, (state: ParameterState) => state.saving);

export const selectSaving = createSelector(selectParameterState, (state: ParameterState) => state.saving);

export const selectParameterContextStatus = createSelector(
    selectParameterState,
    (state: ParameterState) => state.status
);
