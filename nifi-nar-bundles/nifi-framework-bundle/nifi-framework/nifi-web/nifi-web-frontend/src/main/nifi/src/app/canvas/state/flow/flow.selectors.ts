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

import { CanvasState, FlowState } from '../index';
import { createSelector } from '@ngrx/store';

export const selectFlowState = (state: CanvasState) => state.flowState;

export const selectFlowLoadingStatus = createSelector(selectFlowState, (state: FlowState) => state.status);

export const selectFlow = createSelector(selectFlowState, (state: FlowState) => state.flow);

export const selectFlowLoadingError = createSelector(selectFlowState, (state: FlowState) => state.error);

export const selectCurrentProcessGroupId = createSelector(selectFlowState, (state: FlowState) => state.id);

export const selectSelected = createSelector(selectFlowState, (state: FlowState) => state.selection);

export const selectTransitionRequired = createSelector(selectFlowState, (state: FlowState) => state.transitionRequired);

export const selectRenderRequired = createSelector(selectFlowState, (state: FlowState) => state.renderRequired);

export const selectFunnels = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.funnels
);

export const selectProcessors = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.processors
);

export const selectProcessGroups = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.processGroups
);

export const selectRemoteProcessGroups = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.remoteProcessGroups
);

export const selectInputPorts = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.inputPorts
);

export const selectOutputPorts = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.outputPorts
);

export const selectPorts = createSelector(
  selectInputPorts,
  selectOutputPorts,
  (inputPorts: any, outputPorts: any) => [
    ...inputPorts,
    ...outputPorts
]);

export const selectLabels = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.labels
);
