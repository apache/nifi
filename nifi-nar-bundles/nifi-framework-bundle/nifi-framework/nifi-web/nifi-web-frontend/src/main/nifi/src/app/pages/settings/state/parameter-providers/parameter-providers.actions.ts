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

import { createAction, props } from '@ngrx/store';
import {
    ConfigureParameterProviderRequest,
    ConfigureParameterProviderSuccess,
    CreateParameterProviderRequest,
    CreateParameterProviderSuccessResponse,
    DeleteParameterProviderRequest,
    DeleteParameterProviderSuccess,
    EditParameterProviderRequest,
    LoadParameterProvidersResponse,
    SelectParameterProviderRequest
} from './index';

const PARAMETER_PROVIDERS_PREFIX = '[Parameter Providers]';

export const resetParameterProvidersState = createAction(`${PARAMETER_PROVIDERS_PREFIX} Reset Parameter Providers`);

export const loadParameterProviders = createAction(`${PARAMETER_PROVIDERS_PREFIX} Load Parameter Providers`);

export const loadParameterProvidersSuccess = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Load Parameter Providers Success`,
    props<{ response: LoadParameterProvidersResponse }>()
);

export const parameterProvidersApiError = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Load Parameter Providers Error`,
    props<{ error: string }>()
);

export const selectParameterProvider = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Select Parameter Provider`,
    props<{ request: SelectParameterProviderRequest }>()
);

export const openNewParameterProviderDialog = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Open New Parameter Provider Dialog`
);

export const createParameterProvider = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Create Parameter Provider`,
    props<{ request: CreateParameterProviderRequest }>()
);

export const createParameterProviderSuccess = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Create Parameter Provider Success`,
    props<{ response: CreateParameterProviderSuccessResponse }>()
);

export const promptParameterProviderDeletion = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Prompt Parameter Provider Deletion`,
    props<{ request: DeleteParameterProviderRequest }>()
);

export const deleteParameterProvider = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Delete Parameter Provider`,
    props<{ request: DeleteParameterProviderRequest }>()
);

export const deleteParameterProviderSuccess = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Delete Parameter Provider Success`,
    props<{ response: DeleteParameterProviderSuccess }>()
);

export const navigateToEditParameterProvider = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Navigate To Edit Parameter Provider`,
    props<{ id: string }>()
);

export const openConfigureParameterProviderDialog = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Open Configure Parameter Provider Dialog`,
    props<{ request: EditParameterProviderRequest }>()
);

export const configureParameterProvider = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Configure Parameter Provider`,
    props<{ request: ConfigureParameterProviderRequest }>()
);

export const configureParameterProviderSuccess = createAction(
    `${PARAMETER_PROVIDERS_PREFIX} Configure Parameter Provider Success`,
    props<{ response: ConfigureParameterProviderSuccess }>()
);
