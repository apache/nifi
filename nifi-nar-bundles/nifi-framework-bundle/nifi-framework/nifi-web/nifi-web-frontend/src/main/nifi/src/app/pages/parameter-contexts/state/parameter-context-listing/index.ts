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

import { Permissions, Revision } from '../../../../state/shared';

export const parameterContextListingFeatureKey = 'parameterContextListing';

export interface LoadParameterContextsResponse {
    parameterContexts: ParameterContextEntity[];
    loadedTimestamp: string;
}

export interface CreateParameterContext {
    payload: any;
}

export interface CreateParameterContextSuccess {
    parameterContext: ParameterContextEntity;
}

export interface EditParameterContextRequest {
    parameterContext?: ParameterContextEntity;
}

export interface SubmitParameterContextUpdate {
    id: string;
    payload: any;
}

export interface PollParameterContextUpdate {
    uri: string;
}

export interface PollParameterContextUpdateSuccess {
    request: ParameterContextUpdateRequest;
}

export interface DeleteParameterContextUpdate {
    uri: string;
}

export interface ParameterContextUpdateSuccess {
    id: string;
    parameterContext: ParameterContextEntity;
}

export interface DeleteParameterContext {
    parameterContext: ParameterContextEntity;
}

export interface DeleteParameterContextSuccess {
    parameterContext: ParameterContextEntity;
}

export interface SelectParameterContext {
    id: string;
}

export interface ParameterContextUpdateRequest {
    complete: boolean;
    lastUpdated: string;
    percentComponent: number;
    referencingComponents: any[];
    requestId: string;
    state: string;
    updateSteps: any[];
    uri: string;
}

export interface ParameterContextEntity {
    revision: Revision;
    permissions: Permissions;
    id: string;
    uri: string;
    component: any;
}

// export interface ParameterContext {
//     identifier: string;
//     name: string;
//     description: string;
//     parameters: ParameterEntity[];
//     // private Set<ProcessGroupEntity> boundProcessGroups;
//     // private List<ParameterContextReferenceEntity> inheritedParameterContexts;
//     // private ParameterProviderConfigurationEntity parameterProviderConfiguration;
// }
//
export interface ParameterEntity {
    canWrite?: boolean;
    parameter: Parameter;
}

export interface Parameter {
    name: string;
    description: string;
    sensitive: boolean;
    value: string | null;
    valueRemoved?: boolean;
    provided?: boolean;
    // private Set<AffectedComponentEntity> referencingComponents;
    // private ParameterContextReferenceEntity parameterContext;
    inherited?: boolean;
}

export interface ParameterContextListingState {
    parameterContexts: ParameterContextEntity[];
    updateRequest: ParameterContextUpdateRequest | null;
    saving: boolean;
    loadedTimestamp: string;
    error: string | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}
