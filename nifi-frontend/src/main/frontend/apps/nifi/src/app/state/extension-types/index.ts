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

import { DocumentedType } from '../shared';

export const extensionTypesFeatureKey = 'extensionTypes';

export type ExtensionTypesLoadingStatus = 'pending' | 'loading' | 'success' | 'error';

export interface LoadExtensionTypesForCanvasResponse {
    processorTypes: DocumentedType[];
    controllerServiceTypes: DocumentedType[];
    prioritizers: DocumentedType[];
}

export interface LoadExtensionTypesForSettingsResponse {
    controllerServiceTypes: DocumentedType[];
    reportingTaskTypes: DocumentedType[];
    registryClientTypes: DocumentedType[];
    flowAnalysisRuleTypes: DocumentedType[];
    parameterProviderTypes: DocumentedType[];
}

export interface LoadExtensionTypesForPoliciesResponse {
    processorTypes: DocumentedType[];
    controllerServiceTypes: DocumentedType[];
    reportingTaskTypes: DocumentedType[];
    flowAnalysisRuleTypes: DocumentedType[];
    parameterProviderTypes: DocumentedType[];
}

export interface LoadExtensionTypesForDocumentationResponse {
    processorTypes: DocumentedType[];
    controllerServiceTypes: DocumentedType[];
    reportingTaskTypes: DocumentedType[];
    flowAnalysisRuleTypes: DocumentedType[];
    parameterProviderTypes: DocumentedType[];
}

export interface ExtensionTypesState {
    processorTypes: DocumentedType[];
    controllerServiceTypes: DocumentedType[];
    prioritizerTypes: DocumentedType[];
    reportingTaskTypes: DocumentedType[];
    registryClientTypes: DocumentedType[];
    flowAnalysisRuleTypes: DocumentedType[];
    parameterProviderTypes: DocumentedType[];
    status: ExtensionTypesLoadingStatus;
}
