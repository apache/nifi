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

import { ControllerServiceEntity, ControllerServiceReferencingComponentEntity } from '../shared';
import { SelectOption } from '@nifi/shared';

export const controllerServiceStateFeatureKey = 'enableControllerService';

export const controllerServiceActionScopes: SelectOption[] = [
    {
        text: 'Service only',
        value: 'SERVICE_ONLY',
        description: 'Enable only this controller service.'
    },
    {
        text: 'Service and referencing components',
        value: 'SERVICE_AND_REFERENCING_COMPONENTS',
        description: 'Enable this controller service and enable/start all referencing components.'
    }
];

export interface SetControllerServiceRequest {
    controllerService: ControllerServiceEntity;
}

export interface SetEnableControllerServiceRequest {
    scope: 'SERVICE_ONLY' | 'SERVICE_AND_REFERENCING_COMPONENTS';
}

export interface ControllerServiceStepResponse {
    controllerService: ControllerServiceEntity;
    currentStep: SetEnableStep;
}

export interface ReferencingComponentsStepResponse {
    referencingComponents: ControllerServiceReferencingComponentEntity[];
    currentStep: SetEnableStep;
}

export interface SetEnableStepFailure {
    step: SetEnableStep;
    error: string;
}

export enum SetEnableStep {
    Pending,
    EnableService,
    EnableReferencingServices,
    StartReferencingComponents,
    StopReferencingComponents,
    DisableReferencingServices,
    DisableService,
    Completed
}

export interface SetEnableRequest {
    enable: boolean;
    currentStep: SetEnableStep;
    scope: 'SERVICE_ONLY' | 'SERVICE_AND_REFERENCING_COMPONENTS';
    error?: SetEnableStepFailure;
}

export interface ControllerServiceState {
    setEnableRequest: SetEnableRequest;
    controllerService: ControllerServiceEntity | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}
