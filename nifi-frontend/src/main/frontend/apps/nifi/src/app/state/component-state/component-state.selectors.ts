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
import { componentStateFeatureKey, ComponentStateState, ComponentState } from './index';

export const selectComponentStateState = createFeatureSelector<ComponentStateState>(componentStateFeatureKey);

export const selectComponentState = createSelector(
    selectComponentStateState,
    (state: ComponentStateState) => state.componentState
);

export const selectComponentName = createSelector(
    selectComponentStateState,
    (state: ComponentStateState) => state.componentName
);

export const selectComponentType = createSelector(
    selectComponentStateState,
    (state: ComponentStateState) => state.componentType
);

export const selectComponentId = createSelector(
    selectComponentStateState,
    (state: ComponentStateState) => state.componentId
);

export const selectCanClear = createSelector(selectComponentStateState, (state: ComponentStateState) => state.canClear);

export const selectClearing = createSelector(selectComponentStateState, (state: ComponentStateState) => state.clearing);

export const selectDropStateKeySupported = createSelector(
    selectComponentState,
    (componentState: ComponentState | null) => componentState?.dropStateKeySupported ?? false
);
