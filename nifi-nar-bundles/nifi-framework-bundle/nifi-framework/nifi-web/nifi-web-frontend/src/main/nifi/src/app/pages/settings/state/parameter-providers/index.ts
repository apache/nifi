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
    DocumentedType,
    ParameterContextReferenceEntity,
    Permissions,
    PropertyDescriptor,
    Revision
} from '../../../../state/shared';

export const parameterProvidersFeatureKey = 'parameterProviders';

export interface ParameterProvider {
    bundle: Bundle;
    comments: string;
    deprecated: boolean;
    descriptors: { [key: string]: PropertyDescriptor };
    extensionMissing: boolean;
    id: string;
    multipleVersionsAvailable: boolean;
    name: string;
    parameterGroupConfigurations: any[];
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

export interface ParameterProvidersState {
    parameterProviders: ParameterProviderEntity[];
    saving: boolean;
    loadedTimestamp: string;
    error: string | null;
    status: 'pending' | 'loading' | 'error' | 'success';
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
}

export interface ConfigureParameterProviderRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
}

export interface ConfigureParameterProviderSuccess {
    id: string;
    parameterProvider: ParameterProviderEntity;
    postUpdateNavigation?: string[];
}

export interface UpdateParameterProviderRequest {
    payload: any;
    postUpdateNavigation?: string[];
}
