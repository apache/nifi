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

import { ControllerServiceEntity } from '@nifi/shared';
import { BreadcrumbEntity } from '../../../../state/shared';

/**
 * Connector Controller Services State
 *
 * Manages the controller services viewed within a connector's flow. The view is
 * read-only out of TROUBLESHOOTING and editable while the connector is in
 * TROUBLESHOOTING (create / configure / enable / disable / change-version / delete /
 * clear-bulletins). The `saving` flag tracks an in-flight write operation so the dialogs
 * can render their saving state via {@link selectConnectorControllerServicesSaving}.
 */
export const connectorControllerServicesFeatureKey = 'connectorControllerServices';

/**
 * Request to load controller services for a connector's process group.
 */
export interface LoadConnectorControllerServicesRequest {
    connectorId: string;
    processGroupId: string;
}

/**
 * Response from loading controller services.
 */
export interface LoadConnectorControllerServicesResponse {
    connectorId: string;
    processGroupId: string;
    breadcrumb: BreadcrumbEntity | null;
    controllerServices: ControllerServiceEntity[];
    loadedTimestamp: string;
}

/**
 * Payload for controller-service creation success.
 */
export interface CreateControllerServiceSuccess {
    controllerService: ControllerServiceEntity;
}

/**
 * Request to select a controller service (for route-based selection).
 */
export interface SelectConnectorControllerServiceRequest {
    connectorId: string;
    processGroupId: string;
    serviceId: string;
}

/**
 * State for the connector controller services page.
 */
export interface ConnectorControllerServicesState {
    connectorId: string;
    processGroupId: string;
    breadcrumb: BreadcrumbEntity | null;
    controllerServices: ControllerServiceEntity[];
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success' | 'error';
    error: string | null;
    /**
     * Whether at least one load attempt has resolved (in either success or
     * failure). Used by the view to distinguish the very first load (where a
     * skeleton placeholder is appropriate) from subsequent reloads (where the
     * existing content remains in place with an inline spinner).
     */
    hasAttemptedLoad: boolean;
    /**
     * True while a write operation (create, update, delete, clear bulletins)
     * is in flight so editable dialogs can show a spinner.
     */
    saving: boolean;
}

export const initialConnectorControllerServicesState: ConnectorControllerServicesState = {
    connectorId: '',
    processGroupId: '',
    breadcrumb: null,
    controllerServices: [],
    loadedTimestamp: '',
    status: 'pending',
    error: null,
    hasAttemptedLoad: false,
    saving: false
};
