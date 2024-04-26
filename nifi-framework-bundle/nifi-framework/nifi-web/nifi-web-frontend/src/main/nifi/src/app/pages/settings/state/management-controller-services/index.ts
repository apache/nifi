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

import { ControllerServiceEntity } from '../../../../state/shared';

export const managementControllerServicesFeatureKey = 'managementControllerServices';

export interface LoadManagementControllerServicesResponse {
    controllerServices: ControllerServiceEntity[];
    loadedTimestamp: string;
}

export interface CreateControllerServiceSuccess {
    controllerService: ControllerServiceEntity;
}

export interface ConfigureControllerServiceRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
}

export interface ConfigureControllerServiceSuccess {
    id: string;
    controllerService: ControllerServiceEntity;
    postUpdateNavigation?: string[];
}

export interface DeleteControllerServiceRequest {
    controllerService: ControllerServiceEntity;
}

export interface DeleteControllerServiceSuccess {
    controllerService: ControllerServiceEntity;
}

export interface SelectControllerServiceRequest {
    id: string;
}

export interface ManagementControllerServicesState {
    controllerServices: ControllerServiceEntity[];
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
