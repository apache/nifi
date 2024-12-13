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
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { ConfigureRemotePortRequest, ToggleRemotePortTransmissionRequest } from '../state/manage-remote-ports';
import { Client } from '../../../service/client.service';

@Injectable({ providedIn: 'root' })
export class ManageRemotePortService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getRemotePorts(rpgId: string): Observable<any> {
        return this.httpClient.get(`${ManageRemotePortService.API}/remote-process-groups/${rpgId}`);
    }

    updateRemotePort(configureRemotePortRequest: ConfigureRemotePortRequest): Observable<any> {
        const type =
            configureRemotePortRequest.payload.type === ComponentType.InputPort ? 'input-ports' : 'output-ports';
        return this.httpClient.put(
            `${this.nifiCommon.stripProtocol(configureRemotePortRequest.uri)}/${type}/${
                configureRemotePortRequest.payload.remoteProcessGroupPort.id
            }`,
            {
                revision: configureRemotePortRequest.payload.revision,
                remoteProcessGroupPort: configureRemotePortRequest.payload.remoteProcessGroupPort,
                disconnectedNodeAcknowledged: configureRemotePortRequest.payload.disconnectedNodeAcknowledged
            }
        );
    }

    updateRemotePortTransmission(
        toggleRemotePortTransmissionRequest: ToggleRemotePortTransmissionRequest
    ): Observable<any> {
        const payload: any = {
            revision: this.client.getRevision(toggleRemotePortTransmissionRequest.rpg),
            disconnectedNodeAcknowledged: toggleRemotePortTransmissionRequest.disconnectedNodeAcknowledged,
            state: toggleRemotePortTransmissionRequest.state
        };

        const type =
            toggleRemotePortTransmissionRequest.type === ComponentType.InputPort ? 'input-ports' : 'output-ports';

        return this.httpClient.put(
            `${ManageRemotePortService.API}/remote-process-groups/${toggleRemotePortTransmissionRequest.rpg.id}/${type}/${toggleRemotePortTransmissionRequest.portId}/run-status`,
            payload
        );
    }
}
