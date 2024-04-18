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
    ConfigureRemotePortRequest,
    ConfigureRemotePortSuccess,
    EditRemotePortDialogRequest,
    LoadRemotePortsRequest,
    LoadRemotePortsResponse,
    SelectRemotePortRequest,
    StartRemotePortTransmissionRequest,
    StopRemotePortTransmissionRequest
} from './index';

export const resetRemotePortsState = createAction('[Manage Remote Ports] Reset Remote Ports State');

export const loadRemotePorts = createAction(
    '[Manage Remote Ports] Load Remote Ports',
    props<{ request: LoadRemotePortsRequest }>()
);

export const loadRemotePortsSuccess = createAction(
    '[Manage Remote Ports] Load Remote Ports Success',
    props<{ response: LoadRemotePortsResponse }>()
);

export const remotePortsBannerApiError = createAction(
    '[Manage Remote Ports] Remote Ports Banner Api Error',
    props<{ error: string }>()
);

export const navigateToEditPort = createAction('[Manage Remote Ports] Navigate To Edit Port', props<{ id: string }>());

export const openConfigureRemotePortDialog = createAction(
    '[Manage Remote Ports] Open Configure Port Dialog',
    props<{ request: EditRemotePortDialogRequest }>()
);

export const configureRemotePort = createAction(
    '[Manage Remote Ports] Configure Port',
    props<{ request: ConfigureRemotePortRequest }>()
);

export const configureRemotePortSuccess = createAction(
    '[Manage Remote Ports] Configure Port Success',
    props<{ response: ConfigureRemotePortSuccess }>()
);

export const startRemotePortTransmission = createAction(
    '[Manage Remote Ports] Start Port Transmission',
    props<{ request: StartRemotePortTransmissionRequest }>()
);

export const stopRemotePortTransmission = createAction(
    '[Manage Remote Ports] Stop Port Transmission',
    props<{ request: StopRemotePortTransmissionRequest }>()
);

export const selectRemotePort = createAction(
    '[Manage Remote Ports] Select Port Summary',
    props<{ request: SelectRemotePortRequest }>()
);
