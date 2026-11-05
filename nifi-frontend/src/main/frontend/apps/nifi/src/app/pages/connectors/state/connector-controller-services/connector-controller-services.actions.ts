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
import { ControllerServiceEntity } from '@nifi/shared';
import { ErrorContext } from '../../../../state/error';
import {
    CreateControllerServiceSuccess,
    LoadConnectorControllerServicesRequest,
    LoadConnectorControllerServicesResponse,
    SelectConnectorControllerServiceRequest
} from './index';
import {
    ClearBulletinsRequest,
    CreateControllerServiceRequest,
    DisableControllerServiceDialogRequest,
    EditControllerServiceDialogRequest,
    FetchComponentVersionsRequest,
    SetEnableControllerServiceDialogRequest
} from '../../../../state/shared';
import { ConfigureControllerServiceRequest, DeleteControllerServiceRequest } from '../../../../state/flow-shared';

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

/**
 * Navigate to a controller service defined by another process group.
 */
export const navigateToConnectorControllerService = createAction(
    '[Connector Controller Services] Navigate To Controller Service',
    props<{ processGroupId: string; serviceId: string }>()
);

// ─── Troubleshooting-mode: entity polling ───────────────────────────────────

export const startConnectorControllerServicesEntityPolling = createAction(
    '[Connector Controller Services] Start Entity Polling',
    props<{ connectorId: string }>()
);

export const stopConnectorControllerServicesEntityPolling = createAction(
    '[Connector Controller Services] Stop Entity Polling'
);

// ─── Troubleshooting-mode: Create ────────────────────────────────────────────

export const openNewConnectorControllerServiceDialog = createAction(
    '[Connector Controller Services] Open New Controller Service Dialog'
);

export const createConnectorControllerService = createAction(
    '[Connector Controller Services] Create Controller Service',
    props<{ request: CreateControllerServiceRequest }>()
);

export const createConnectorControllerServiceSuccess = createAction(
    '[Connector Controller Services] Create Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);

export const createConnectorControllerServiceFailure = createAction(
    '[Connector Controller Services] Create Controller Service Failure',
    props<{ error: string }>()
);

// ─── Troubleshooting-mode: Configure (editable) ──────────────────────────────

export const openConfigureConnectorControllerServiceDialog = createAction(
    '[Connector Controller Services] Open Configure Controller Service Dialog',
    props<{ request: EditControllerServiceDialogRequest }>()
);

export const saveConnectorControllerService = createAction(
    '[Connector Controller Services] Save Controller Service',
    props<{ request: ConfigureControllerServiceRequest }>()
);

export const saveConnectorControllerServiceSuccess = createAction(
    '[Connector Controller Services] Save Controller Service Success',
    props<{ response: ControllerServiceEntity }>()
);

export const saveConnectorControllerServiceFailure = createAction(
    '[Connector Controller Services] Save Controller Service Failure',
    props<{ error: string }>()
);

// ─── Troubleshooting-mode: Enable ────────────────────────────────────────────

export const openEnableConnectorControllerServiceDialog = createAction(
    '[Connector Controller Services] Open Enable Controller Service Dialog',
    props<{ request: SetEnableControllerServiceDialogRequest }>()
);

// ─── Troubleshooting-mode: Disable ───────────────────────────────────────────

export const openDisableConnectorControllerServiceDialog = createAction(
    '[Connector Controller Services] Open Disable Controller Service Dialog',
    props<{ request: DisableControllerServiceDialogRequest }>()
);

// ─── Troubleshooting-mode: Change Version ────────────────────────────────────

export const openChangeConnectorControllerServiceVersionDialog = createAction(
    '[Connector Controller Services] Open Change Controller Service Version Dialog',
    props<{ request: FetchComponentVersionsRequest }>()
);

// ─── Troubleshooting-mode: Delete ────────────────────────────────────────────

export const promptConnectorControllerServiceDeletion = createAction(
    '[Connector Controller Services] Prompt Controller Service Deletion',
    props<{ request: DeleteControllerServiceRequest }>()
);

export const deleteConnectorControllerService = createAction(
    '[Connector Controller Services] Delete Controller Service',
    props<{ request: DeleteControllerServiceRequest }>()
);

export const deleteConnectorControllerServiceSuccess = createAction(
    '[Connector Controller Services] Delete Controller Service Success'
);

export const deleteConnectorControllerServiceFailure = createAction(
    '[Connector Controller Services] Delete Controller Service Failure',
    props<{ error: string }>()
);

// ─── Troubleshooting-mode: Clear Bulletins ───────────────────────────────────

export const clearConnectorControllerServiceBulletins = createAction(
    '[Connector Controller Services] Clear Controller Service Bulletins',
    props<{ request: ClearBulletinsRequest }>()
);

export const clearConnectorControllerServiceBulletinsSuccess = createAction(
    '[Connector Controller Services] Clear Controller Service Bulletins Success'
);

export const clearConnectorControllerServiceBulletinsFailure = createAction(
    '[Connector Controller Services] Clear Controller Service Bulletins Failure',
    props<{ error: string }>()
);

// ─── Troubleshooting-mode: Advanced UI ───────────────────────────────────────

export const navigateToAdvancedConnectorControllerServiceUi = createAction(
    '[Connector Controller Services] Navigate To Advanced Controller Service UI',
    props<{ id: string }>()
);

/**
 * Notifies the connector controller-services slice that a controller service
 * was created inline from a connector edit dialog. The HTTP create is performed
 * by `PropertyTableHelperService.createNewService`; this action keeps the
 * in-memory listing in sync without a server round-trip.
 */
export const inlineCreateConnectorControllerServiceSuccess = createAction(
    '[Connector Controller Services] Inline Create Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);
