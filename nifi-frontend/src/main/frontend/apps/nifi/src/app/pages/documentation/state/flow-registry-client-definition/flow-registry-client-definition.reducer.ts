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

import { createReducer, on } from '@ngrx/store';
import { FlowRegistryClientDefinitionState } from './index';
import {
    flowRegistryClientDefinitionApiError,
    loadFlowRegistryClientDefinition,
    loadFlowRegistryClientDefinitionSuccess,
    resetFlowRegistryClientDefinitionState
} from './flow-registry-client-definition.actions';

export const initialFlowRegistryClientDefinitionState: FlowRegistryClientDefinitionState = {
    flowRegistryClientDefinition: null,
    error: null,
    status: 'pending'
};

export const flowRegistryClientDefinitionReducer = createReducer(
    initialFlowRegistryClientDefinitionState,
    on(loadFlowRegistryClientDefinition, (state) => ({
        ...state,
        flowRegistryClientDefinition: null,
        error: null,
        status: 'loading' as const
    })),
    on(loadFlowRegistryClientDefinitionSuccess, (state, { flowRegistryClientDefinition }) => ({
        ...state,
        flowRegistryClientDefinition,
        error: null,
        status: 'success' as const
    })),
    on(flowRegistryClientDefinitionApiError, (state, { error }) => ({
        ...state,
        error,
        status: 'error' as const
    })),
    on(resetFlowRegistryClientDefinitionState, () => ({
        ...initialFlowRegistryClientDefinitionState
    }))
);
