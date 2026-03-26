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
import { ConnectorDefinitionState } from './index';
import {
    connectorDefinitionApiError,
    loadConnectorDefinition,
    loadConnectorDefinitionSuccess,
    loadStepDocumentation,
    loadStepDocumentationSuccess,
    resetConnectorDefinitionState,
    stepDocumentationApiError
} from './connector-definition.actions';

export const initialConnectorDefinitionState: ConnectorDefinitionState = {
    connectorDefinition: null,
    error: null,
    status: 'pending',
    stepDocumentation: {}
};

export const connectorDefinitionReducer = createReducer(
    initialConnectorDefinitionState,
    on(loadConnectorDefinition, (state) => ({
        ...state,
        connectorDefinition: null,
        error: null,
        status: 'loading' as const
    })),
    on(loadConnectorDefinitionSuccess, (state, { connectorDefinition }) => ({
        ...state,
        connectorDefinition,
        error: null,
        status: 'success' as const
    })),
    on(connectorDefinitionApiError, (state, { error }) => ({
        ...state,
        error,
        status: 'error' as const
    })),
    on(resetConnectorDefinitionState, () => ({
        ...initialConnectorDefinitionState
    })),
    on(loadStepDocumentation, (state, { stepName }) => ({
        ...state,
        stepDocumentation: {
            ...state.stepDocumentation,
            [stepName]: {
                documentation: null,
                error: null,
                status: 'loading' as const
            }
        }
    })),
    on(loadStepDocumentationSuccess, (state, { stepName, documentation }) => ({
        ...state,
        stepDocumentation: {
            ...state.stepDocumentation,
            [stepName]: {
                documentation,
                error: null,
                status: 'success' as const
            }
        }
    })),
    on(stepDocumentationApiError, (state, { stepName, error }) => ({
        ...state,
        stepDocumentation: {
            ...state.stepDocumentation,
            [stepName]: {
                documentation: null,
                error,
                status: 'error' as const
            }
        }
    }))
);
