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

import { createAction, props } from '@ngrx/store';
import {
    SetEnableControllerServiceRequest,
    ReferencingComponentsStepResponse,
    ControllerServiceStepResponse,
    SetEnableStepFailure,
    SetControllerServiceRequest,
    SetEnableStep
} from './index';

export const resetEnableControllerServiceState = createAction(
    '[Enable Controller Service] Reset Enable Controller Service State'
);

export const setControllerService = createAction(
    '[Enable Controller Service] Set Controller Service',
    props<{
        request: SetControllerServiceRequest;
    }>()
);

export const submitEnableRequest = createAction(
    '[Enable Controller Service] Submit Enable Request',
    props<{
        request: SetEnableControllerServiceRequest;
    }>()
);

export const submitDisableRequest = createAction('[Enable Controller Service] Submit Disable Request');

export const setEnableControllerService = createAction('[Enable Controller Service] Set Enable Controller Service');

export const setEnableControllerServiceSuccess = createAction(
    '[Enable Controller Service] Set Enable Controller Service Success',
    props<{
        response: ControllerServiceStepResponse;
    }>()
);

export const updateReferencingServices = createAction('[Enable Controller Service] Update Referencing Services');

export const updateReferencingServicesSuccess = createAction(
    '[Enable Controller Service] Update Referencing Services Success',
    props<{
        response: ReferencingComponentsStepResponse;
    }>()
);

export const updateReferencingComponents = createAction('[Enable Controller Service] Update Referencing Components');

export const updateReferencingComponentsSuccess = createAction(
    '[Enable Controller Service] Update Referencing Components Success',
    props<{
        response: ReferencingComponentsStepResponse;
    }>()
);

export const startPollingControllerService = createAction(
    '[Enable Controller Service] Start Polling Controller Service'
);

export const stopPollingControllerService = createAction('[Enable Controller Service] Stop Polling Controller Service');

export const pollControllerService = createAction('[Enable Controller Service] Poll Controller Service');

export const pollControllerServiceSuccess = createAction(
    '[Enable Controller Service] Poll Controller Service Success',
    props<{
        response: ControllerServiceStepResponse;
        previousStep: SetEnableStep;
    }>()
);

export const setEnableStepFailure = createAction(
    '[Enable Controller Service] Set Enable Step Failure',
    props<{
        response: SetEnableStepFailure;
    }>()
);

export const controllerServiceApiError = createAction(
    '[Enable Controller Service] Controller Service Api Error',
    props<{
        error: string;
    }>()
);

export const clearControllerServiceApiError = createAction(
    '[Enable Controller Service] Clear Controller Service Api Error'
);

export const showOkDialog = createAction(
    '[Enable Controller Service] Show Ok Dialog',
    props<{
        title: string;
        message: string;
    }>()
);
