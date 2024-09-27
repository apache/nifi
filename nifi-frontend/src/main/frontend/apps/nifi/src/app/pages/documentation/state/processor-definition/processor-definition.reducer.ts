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

import { ProcessorDefinitionState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    loadProcessorDefinition,
    loadProcessorDefinitionSuccess,
    processorDefinitionApiError,
    resetProcessorDefinitionState
} from './processor-definition.actions';

export const initialState: ProcessorDefinitionState = {
    processorDefinition: null,
    error: null,
    status: 'pending'
};

export const processorDefinitionReducer = createReducer(
    initialState,
    on(loadProcessorDefinition, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadProcessorDefinitionSuccess, (state, { processorDefinition }) => ({
        ...state,
        processorDefinition,
        error: null,
        status: 'success' as const
    })),
    on(processorDefinitionApiError, (state, { error }) => ({
        ...state,
        processorDefinition: null,
        error,
        status: 'error' as const
    })),
    on(resetProcessorDefinitionState, () => ({
        ...initialState
    }))
);
