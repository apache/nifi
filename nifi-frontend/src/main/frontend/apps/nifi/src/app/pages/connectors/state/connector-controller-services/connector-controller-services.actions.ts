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
import { ErrorContext } from '../../../../state/error';
import { ControllerServiceEntity } from '../../../../state/shared';
import {
    LoadConnectorControllerServicesRequest,
    LoadConnectorControllerServicesResponse,
    SelectConnectorControllerServiceRequest
} from './index';

/**
 * Load controller services for a connector's process group.
 */
export const loadConnectorControllerServices = createAction(
    '[Connector Controller Services] Load Controller Services',
    props<{ request: LoadConnectorControllerServicesRequest }>()
);

export const loadConnectorControllerServicesSuccess = createAction(
    '[Connector Controller Services] Load Controller Services Success',
    props<{ response: LoadConnectorControllerServicesResponse }>()
);

export const loadConnectorControllerServicesFailure = createAction(
    '[Connector Controller Services] Load Controller Services Failure',
    props<{ errorContext: ErrorContext }>()
);

/**
 * Select a controller service (updates route for deep linking).
 */
export const selectConnectorControllerService = createAction(
    '[Connector Controller Services] Select Controller Service',
    props<{ request: SelectConnectorControllerServiceRequest }>()
);

/**
 * Open the read-only view configuration dialog for a controller service.
 */
export const openViewControllerServiceDialog = createAction(
    '[Connector Controller Services] Open View Controller Service Dialog',
    props<{ controllerService: ControllerServiceEntity }>()
);

/**
 * Reset state when leaving the page.
 */
export const resetConnectorControllerServicesState = createAction('[Connector Controller Services] Reset State');
