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
    '[Controller Service State] Reset Enable Controller Service State'
);

export const setControllerService = createAction(
    '[Controller Service State] Set Controller Service',
    props<{
        request: SetControllerServiceRequest;
    }>()
);

export const submitEnableRequest = createAction(
    '[Controller Service State] Submit Enable Request',
    props<{
        request: SetEnableControllerServiceRequest;
    }>()
);

export const submitDisableRequest = createAction('[Controller Service State] Submit Disable Request');

export const setEnableControllerService = createAction('[Controller Service State] Set Enable Controller Service');

export const setEnableControllerServiceSuccess = createAction(
    '[Controller Service State] Set Enable Controller Service Success',
    props<{
        response: ControllerServiceStepResponse;
    }>()
);

export const updateReferencingServices = createAction('[Controller Service State] Update Referencing Services');

export const updateReferencingServicesSuccess = createAction(
    '[Controller Service State] Update Referencing Services Success',
    props<{
        response: ReferencingComponentsStepResponse;
    }>()
);

export const updateReferencingComponents = createAction('[Controller Service State] Update Referencing Components');

export const updateReferencingComponentsSuccess = createAction(
    '[Controller Service State] Update Referencing Components Success',
    props<{
        response: ReferencingComponentsStepResponse;
    }>()
);

export const startPollingControllerService = createAction(
    '[Controller Service State] Start Polling Controller Service'
);

export const stopPollingControllerService = createAction('[Controller Service State] Stop Polling Controller Service');

export const pollControllerService = createAction('[Controller Service State] Poll Controller Service');

export const pollControllerServiceSuccess = createAction(
    '[Controller Service State] Poll Controller Service Success',
    props<{
        response: ControllerServiceStepResponse;
        previousStep: SetEnableStep;
    }>()
);

export const setEnableStepFailure = createAction(
    '[Controller Service State] Set Enable Step Failure',
    props<{
        response: SetEnableStepFailure;
    }>()
);

export const showOkDialog = createAction(
    '[Controller Service State] Show Ok Dialog',
    props<{
        title: string;
        message: string;
    }>()
);
