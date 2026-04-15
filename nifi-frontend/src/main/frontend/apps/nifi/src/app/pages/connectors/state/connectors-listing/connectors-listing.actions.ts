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
import { HttpErrorResponse } from '@angular/common/http';
import {
    CancelDrainConnectorRequest,
    ConfigureConnectorRequest,
    ConnectorActionSuccess,
    CreateConnectorRequest,
    CreateConnectorSuccess,
    DeleteConnectorRequest,
    DeleteConnectorSuccess,
    DiscardConnectorConfigRequest,
    DrainConnectorRequest,
    LoadConnectorsListingResponse,
    RenameConnectorRequest,
    RenameConnectorSuccess,
    SelectConnectorRequest,
    StartConnectorRequest,
    StartConnectorSuccess,
    StopConnectorRequest,
    StopConnectorSuccess,
    ViewConnectorRequest
} from '../index';
import { ConnectorEntity } from '@nifi/shared';

export const resetConnectorsListingState = createAction('[Connectors Listing] Reset Connectors Listing State');

export const loadConnectorsListing = createAction('[Connectors Listing] Load Connectors Listing');

export const loadConnectorsListingSuccess = createAction(
    '[Connectors Listing] Load Connectors Listing Success',
    props<{ response: LoadConnectorsListingResponse }>()
);

export const loadConnectorsListingError = createAction(
    '[Connectors Listing] Load Connectors Listing Error',
    props<{ errorResponse: HttpErrorResponse; loadedTimestamp: string; status: 'pending' | 'success' }>()
);

export const connectorsListingBannerApiError = createAction(
    '[Connectors Listing] Connectors Listing Banner Api Error',
    props<{ error: string }>()
);

export const openNewConnectorDialog = createAction('[Connectors Listing] Open New Connector Dialog');

export const createConnector = createAction(
    '[Connectors Listing] Create Connector',
    props<{ request: CreateConnectorRequest }>()
);

export const createConnectorSuccess = createAction(
    '[Connectors Listing] Create Connector Success',
    props<{ response: CreateConnectorSuccess }>()
);

export const navigateToConfigureConnector = createAction(
    '[Connectors Listing] Navigate To Configure Connector',
    props<{ request: ConfigureConnectorRequest }>()
);

export const promptConnectorDeletion = createAction(
    '[Connectors Listing] Prompt Connector Deletion',
    props<{ request: DeleteConnectorRequest }>()
);

export const deleteConnector = createAction(
    '[Connectors Listing] Delete Connector',
    props<{ request: DeleteConnectorRequest }>()
);

export const deleteConnectorSuccess = createAction(
    '[Connectors Listing] Delete Connector Success',
    props<{ response: DeleteConnectorSuccess }>()
);

export const selectConnector = createAction(
    '[Connectors Listing] Select Connector',
    props<{ request: SelectConnectorRequest }>()
);

export const startConnector = createAction(
    '[Connectors Listing] Start Connector',
    props<{ request: StartConnectorRequest }>()
);

export const startConnectorSuccess = createAction(
    '[Connectors Listing] Start Connector Success',
    props<{ response: StartConnectorSuccess }>()
);

export const stopConnector = createAction(
    '[Connectors Listing] Stop Connector',
    props<{ request: StopConnectorRequest }>()
);

export const stopConnectorSuccess = createAction(
    '[Connectors Listing] Stop Connector Success',
    props<{ response: StopConnectorSuccess }>()
);

export const navigateToManageAccessPolicies = createAction(
    '[Connectors Listing] Navigate To Manage Access Policies',
    props<{ id: string }>()
);

export const navigateToViewConnector = createAction(
    '[Connectors Listing] Navigate To View Connector',
    props<{ request: ViewConnectorRequest }>()
);

export const navigateToViewConnectorDetails = createAction(
    '[Connectors Listing] Navigate To View Connector Details',
    props<{ id: string }>()
);

export const openRenameConnectorDialog = createAction(
    '[Connectors Listing] Open Rename Connector Dialog',
    props<{ connector: ConnectorEntity }>()
);

export const renameConnector = createAction(
    '[Connectors Listing] Rename Connector',
    props<{ request: RenameConnectorRequest }>()
);

export const renameConnectorSuccess = createAction(
    '[Connectors Listing] Rename Connector Success',
    props<{ response: RenameConnectorSuccess }>()
);

export const renameConnectorApiError = createAction(
    '[Connectors Listing] Rename Connector Api Error',
    props<{ error: string }>()
);

export const promptDiscardConnectorConfig = createAction(
    '[Connectors Listing] Prompt Discard Connector Config',
    props<{ request: DiscardConnectorConfigRequest }>()
);

export const discardConnectorConfig = createAction(
    '[Connectors Listing] Discard Connector Config',
    props<{ request: DiscardConnectorConfigRequest }>()
);

export const discardConnectorConfigSuccess = createAction(
    '[Connectors Listing] Discard Connector Config Success',
    props<{ response: ConnectorActionSuccess }>()
);

export const promptDrainConnector = createAction(
    '[Connectors Listing] Prompt Drain Connector',
    props<{ request: DrainConnectorRequest }>()
);

export const drainConnector = createAction(
    '[Connectors Listing] Drain Connector',
    props<{ request: DrainConnectorRequest }>()
);

export const drainConnectorSuccess = createAction(
    '[Connectors Listing] Drain Connector Success',
    props<{ response: ConnectorActionSuccess }>()
);

export const cancelConnectorDrain = createAction(
    '[Connectors Listing] Cancel Connector Drain',
    props<{ request: CancelDrainConnectorRequest }>()
);

export const cancelConnectorDrainSuccess = createAction(
    '[Connectors Listing] Cancel Connector Drain Success',
    props<{ response: ConnectorActionSuccess }>()
);
