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

import {
    Bundle,
    ComponentHistory,
    DocumentedType,
    ParameterEntity,
    PropertyDescriptor
} from '../../../../state/shared';
import { AffectedComponentEntity, ParameterContextReferenceEntity, Permissions, Revision } from '@nifi/shared';

export const parameterProvidersFeatureKey = 'parameterProviders';

export interface ParameterSensitivity {
    name: string;
    sensitive: boolean;
}

export interface ParameterGroupConfiguration {
    groupName: string;
    parameterContextName: string;
    parameterSensitivities: { [key: string]: null | 'SENSITIVE' | 'NON_SENSITIVE' };
    synchronized?: boolean;
}

export interface ParameterStatusEntity {
    parameter?: ParameterEntity;
    status: 'NEW' | 'CHANGED' | 'REMOVED' | 'MISSING_BUT_REFERENCED' | 'UNCHANGED';
}

export interface FetchedParameterMapping {
    name: string;
    sensitivity?: ParameterSensitivity;
    status?: ParameterStatusEntity;
}

export interface ParameterProvider {
    affectedComponents: AffectedComponentEntity[];
    bundle: Bundle;
    comments: string;
    customUiUrl?: string;
    deprecated: boolean;
    descriptors: { [key: string]: PropertyDescriptor };
    extensionMissing: boolean;
    id: string;
    multipleVersionsAvailable: boolean;
    name: string;
    parameterGroupConfigurations: ParameterGroupConfiguration[];
    parameterStatus?: ParameterStatusEntity[];
    persistsState: boolean;
    properties: { [key: string]: string };
    referencingParameterContexts: ParameterContextReferenceEntity[];
    restricted: boolean;
    type: string;
    validationStatus: string;
    validationErrors?: string[];
}

export interface ParameterProviderEntity {
    id: string;
    bulletins: [];
    component: ParameterProvider;
    permissions: Permissions;
    revision: Revision;
    uri: string;
}

export interface ParameterProviderParameterApplicationEntity {
    id: string;
    revision: Revision;
    disconnectedNodeAcknowledged: boolean;
    parameterGroupConfigurations: ParameterGroupConfiguration[];
}

export interface UpdateStep {
    description: string;
    complete: boolean;
    failureReason?: string;
}

export interface ParameterContextUpdateRequest {
    parameterContextRevision: Revision;
    parameterContext: any;
    referencingComponents: AffectedComponentEntity[];
}

// returned from '/apply-parameters-request'
export interface ParameterProviderApplyParametersRequest {
    requestId: string;
    complete: boolean;
    lastUpdated: string;
    percentComplete: number;
    state: string;
    uri: string;
    parameterContextUpdates: ParameterContextUpdateRequest[];
    parameterProvider: ParameterProvider;
    referencingComponents: AffectedComponentEntity[];
    updateSteps: UpdateStep[];
}

export interface ParameterProvidersState {
    parameterProviders: ParameterProviderEntity[];
    fetched: ParameterProviderEntity | null;
    applyParametersRequestEntity: ParameterProviderApplyParametersRequest | null;
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}

export interface LoadParameterProvidersResponse {
    parameterProviders: ParameterProviderEntity[];
    loadedTimestamp: string;
}

export interface SelectParameterProviderRequest {
    id: string;
}

export interface CreateParameterProviderDialogRequest {
    parameterProviderTypes: DocumentedType[];
}

export interface CreateParameterProviderRequest {
    parameterProviderType: string;
    parameterProviderBundle: Bundle;
    revision: Revision;
}

export interface CreateParameterProviderSuccessResponse {
    parameterProvider: ParameterProviderEntity;
}

export interface DeleteParameterProviderRequest {
    parameterProvider: ParameterProviderEntity;
}

export interface DeleteParameterProviderSuccess {
    parameterProvider: ParameterProviderEntity;
}

export interface EditParameterProviderRequest {
    id: string;
    parameterProvider: ParameterProviderEntity;
    history?: ComponentHistory;
}

export interface ConfigureParameterProviderRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface ConfigureParameterProviderSuccess {
    id: string;
    parameterProvider: ParameterProviderEntity;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface UpdateParameterProviderRequest {
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface FetchParameterProviderParametersRequest {
    id: string;
    revision: Revision;
}

export interface FetchParameterProviderParametersResponse {
    parameterProvider: ParameterProviderEntity;
}

export interface FetchParameterProviderDialogRequest {
    id: string;
    parameterProvider: ParameterProviderEntity;
}

export interface PollParameterProviderParametersUpdateSuccess {
    request: ParameterProviderApplyParametersRequest;
}
