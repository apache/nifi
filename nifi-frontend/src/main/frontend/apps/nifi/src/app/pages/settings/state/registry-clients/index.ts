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

import { Bundle, RegistryClientEntity } from '../../../../state/shared';
import { Revision } from '@nifi/shared';

export const registryClientsFeatureKey = 'registryClients';

export interface LoadRegistryClientsResponse {
    registryClients: RegistryClientEntity[];
    loadedTimestamp: string;
}

export interface CreateRegistryClientRequest {
    revision: Revision;
    disconnectedNodeAcknowledged: boolean;
    component: {
        name: string;
        type: string;
        bundle: Bundle;
        description?: string;
    };
}

export interface CreateRegistryClientSuccess {
    registryClient: RegistryClientEntity;
}

export interface EditRegistryClientDialogRequest {
    registryClient: RegistryClientEntity;
}

export interface EditRegistryClientRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface EditRegistryClientRequestSuccess {
    id: string;
    registryClient: RegistryClientEntity;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface DeleteRegistryClientRequest {
    registryClient: RegistryClientEntity;
}

export interface DeleteRegistryClientSuccess {
    registryClient: RegistryClientEntity;
}

export interface SelectRegistryClientRequest {
    id: string;
}

export interface RegistryClientsState {
    registryClients: RegistryClientEntity[];
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
