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

import { createReducer, on } from '@ngrx/store';
import { ExtensionTypesState } from './index';
import {
    loadExtensionTypesForCanvas,
    loadExtensionTypesForCanvasSuccess,
    loadExtensionTypesForDocumentationSuccess,
    loadExtensionTypesForPoliciesSuccess,
    loadExtensionTypesForSettingsSuccess
} from './extension-types.actions';

export const initialState: ExtensionTypesState = {
    processorTypes: [],
    controllerServiceTypes: [],
    prioritizerTypes: [],
    reportingTaskTypes: [],
    registryClientTypes: [],
    flowAnalysisRuleTypes: [],
    parameterProviderTypes: [],
    status: 'pending'
};

export const extensionTypesReducer = createReducer(
    initialState,
    on(loadExtensionTypesForCanvas, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadExtensionTypesForCanvasSuccess, (state, { response }) => ({
        ...state,
        processorTypes: response.processorTypes,
        controllerServiceTypes: response.controllerServiceTypes,
        prioritizerTypes: response.prioritizers,
        status: 'success' as const
    })),
    on(loadExtensionTypesForSettingsSuccess, (state, { response }) => ({
        ...state,
        controllerServiceTypes: response.controllerServiceTypes,
        reportingTaskTypes: response.reportingTaskTypes,
        registryClientTypes: response.registryClientTypes,
        parameterProviderTypes: response.parameterProviderTypes,
        flowAnalysisRuleTypes: response.flowAnalysisRuleTypes,
        status: 'success' as const
    })),
    on(loadExtensionTypesForPoliciesSuccess, (state, { response }) => ({
        ...state,
        processorTypes: response.processorTypes,
        controllerServiceTypes: response.controllerServiceTypes,
        reportingTaskTypes: response.reportingTaskTypes,
        parameterProviderTypes: response.parameterProviderTypes,
        flowAnalysisRuleTypes: response.flowAnalysisRuleTypes,
        status: 'success' as const
    })),
    on(loadExtensionTypesForDocumentationSuccess, (state, { response }) => ({
        ...state,
        processorTypes: response.processorTypes,
        controllerServiceTypes: response.controllerServiceTypes,
        reportingTaskTypes: response.reportingTaskTypes,
        parameterProviderTypes: response.parameterProviderTypes,
        flowAnalysisRuleTypes: response.flowAnalysisRuleTypes,
        status: 'success' as const
    }))
);
