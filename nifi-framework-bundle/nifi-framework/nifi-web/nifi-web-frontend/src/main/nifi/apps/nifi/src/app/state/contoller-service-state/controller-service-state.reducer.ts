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
import { ControllerServiceState, SetEnableStep } from './index';
import {
    setEnableStepFailure,
    pollControllerServiceSuccess,
    resetEnableControllerServiceState,
    setControllerService,
    setEnableControllerServiceSuccess,
    submitDisableRequest,
    submitEnableRequest,
    updateReferencingComponentsSuccess,
    updateReferencingServicesSuccess
} from './controller-service-state.actions';
import { produce } from 'immer';

export const initialState: ControllerServiceState = {
    setEnableRequest: {
        enable: true,
        currentStep: SetEnableStep.Pending,
        scope: 'SERVICE_ONLY'
    },
    controllerService: null,
    status: 'pending'
};

export const controllerServiceStateReducer = createReducer(
    initialState,
    on(resetEnableControllerServiceState, () => ({
        ...initialState
    })),
    on(setControllerService, (state, { request }) => ({
        ...state,
        controllerService: request.controllerService
    })),
    on(submitEnableRequest, (state, { request }) => ({
        ...state,
        setEnableRequest: {
            enable: true,
            currentStep: SetEnableStep.EnableService,
            scope: request.scope
        }
    })),
    on(submitDisableRequest, (state) => ({
        ...state,
        setEnableRequest: {
            enable: false,
            currentStep: SetEnableStep.StopReferencingComponents,
            scope: 'SERVICE_AND_REFERENCING_COMPONENTS' as const
        }
    })),
    on(setEnableControllerServiceSuccess, pollControllerServiceSuccess, (state, { response }) => ({
        ...state,
        controllerService: response.controllerService,
        setEnableRequest: {
            ...state.setEnableRequest,
            currentStep: response.currentStep
        }
    })),
    on(updateReferencingServicesSuccess, updateReferencingComponentsSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            if (draftState.controllerService) {
                draftState.controllerService.component.referencingComponents = response.referencingComponents;
            }
            draftState.setEnableRequest.currentStep = response.currentStep;
        });
    }),
    on(setEnableStepFailure, (state, { response }) => ({
        ...state,
        setEnableRequest: {
            ...state.setEnableRequest,
            error: response
        },
        status: 'error' as const
    }))
);
