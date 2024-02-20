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

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { ConfigureRemotePortRequest, ToggleRemotePortTransmissionRequest } from '../state/manage-remote-ports';
import { Client } from '../../../service/client.service';
import { Storage } from '../../../service/storage.service';
import { ComponentType } from '../../../state/shared';

@Injectable({ providedIn: 'root' })
export class ManageRemotePortService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private storage: Storage,
        private nifiCommon: NiFiCommon
    ) {}

    getRemotePorts(rpgId: string): Observable<any> {
        return this.httpClient.get(`${ManageRemotePortService.API}/remote-process-groups/${rpgId}`);
    }

    updateRemotePort(configureManageRemotePort: ConfigureRemotePortRequest): Observable<any> {
        const type =
            configureManageRemotePort.payload.type === ComponentType.InputPort ? 'input-ports' : 'output-ports';
        return this.httpClient.put(
            `${this.nifiCommon.stripProtocol(configureManageRemotePort.uri)}/${type}/${
                configureManageRemotePort.payload.remoteProcessGroupPort.id
            }`,
            {
                revision: configureManageRemotePort.payload.revision,
                remoteProcessGroupPort: configureManageRemotePort.payload.remoteProcessGroupPort,
                disconnectedNodeAcknowledged: configureManageRemotePort.payload.disconnectedNodeAcknowledged
            }
        );
    }

    togglePortTransmission(toggleRemotePortTransmissionRequest: ToggleRemotePortTransmissionRequest): Observable<any> {
        let isTransmitting = false;
        if (toggleRemotePortTransmissionRequest.port.connected) {
            if (!toggleRemotePortTransmissionRequest.port.transmitting) {
                if (toggleRemotePortTransmissionRequest.port.exists) {
                    isTransmitting = true;
                }
            }
        }

        const payload: any = {
            revision: this.client.getRevision(toggleRemotePortTransmissionRequest.rpg),
            disconnectedNodeAcknowledged: this.storage.isDisconnectionAcknowledged(),
            state: isTransmitting ? 'TRANSMITTING' : 'STOPPED'
        };

        const type =
            toggleRemotePortTransmissionRequest.port.type === ComponentType.InputPort ? 'input-ports' : 'output-ports';
        return this.httpClient.put(
            `${ManageRemotePortService.API}/remote-process-groups/${toggleRemotePortTransmissionRequest.rpg.id}/${type}/${toggleRemotePortTransmissionRequest.port.id}/run-status`,
            payload
        );
    }
}
