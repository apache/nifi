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

import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ConnectorsResponse, CreateConnectorRequest } from '../state';
import { ConnectorEntity } from '@nifi/shared';
import { DropRequestEntity } from '../../flow-designer/state/queue';

@Injectable({ providedIn: 'root' })
export class ConnectorService {
    private httpClient = inject(HttpClient);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    getConnectors(): Observable<ConnectorsResponse> {
        return this.httpClient.get<ConnectorsResponse>(`${ConnectorService.API}/flow/connectors`);
    }

    createConnector(createConnectorRequest: CreateConnectorRequest): Observable<ConnectorEntity> {
        return this.httpClient.post<ConnectorEntity>(`${ConnectorService.API}/connectors`, {
            revision: createConnectorRequest.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                bundle: createConnectorRequest.connectorBundle,
                type: createConnectorRequest.connectorType
            }
        });
    }

    updateConnector(connector: ConnectorEntity): Observable<ConnectorEntity> {
        return this.httpClient.put<ConnectorEntity>(`${ConnectorService.API}/connectors/${connector.id}`, {
            revision: this.client.getRevision(connector),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: connector.component,
            id: connector.id
        });
    }

    deleteConnector(connector: ConnectorEntity): Observable<ConnectorEntity> {
        const revision = this.client.getRevision(connector);
        const params: Record<string, string | number | boolean> = {
            ...revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        };
        return this.httpClient.delete<ConnectorEntity>(`${ConnectorService.API}/connectors/${connector.id}`, {
            params
        });
    }

    updateConnectorRunStatus(connector: ConnectorEntity, runStatus: string): Observable<ConnectorEntity> {
        const revision = this.client.getRevision(connector);
        return this.httpClient.put<ConnectorEntity>(`${ConnectorService.API}/connectors/${connector.id}/run-status`, {
            revision,
            state: runStatus,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        });
    }

    discardConnectorWorkingConfiguration(connector: ConnectorEntity): Observable<ConnectorEntity> {
        const revision = this.client.getRevision(connector);
        const params: Record<string, string | number | boolean> = {
            ...revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        };
        return this.httpClient.delete<ConnectorEntity>(
            `${ConnectorService.API}/connectors/${connector.id}/working-configuration`,
            { params }
        );
    }

    drainConnector(connector: ConnectorEntity): Observable<ConnectorEntity> {
        return this.httpClient.post<ConnectorEntity>(`${ConnectorService.API}/connectors/${connector.id}/drain`, {
            revision: this.client.getRevision(connector),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        });
    }

    cancelConnectorDrain(connector: ConnectorEntity): Observable<ConnectorEntity> {
        const revision = this.client.getRevision(connector);
        const params: Record<string, string | number | boolean> = {
            ...revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        };
        return this.httpClient.delete<ConnectorEntity>(`${ConnectorService.API}/connectors/${connector.id}/drain`, {
            params
        });
    }

    createPurgeRequest(connectorId: string): Observable<DropRequestEntity> {
        return this.httpClient.post<DropRequestEntity>(
            `${ConnectorService.API}/connectors/${connectorId}/purge-requests`,
            {}
        );
    }

    getPurgeRequest(connectorId: string, purgeRequestId: string): Observable<DropRequestEntity> {
        return this.httpClient.get<DropRequestEntity>(
            `${ConnectorService.API}/connectors/${connectorId}/purge-requests/${purgeRequestId}`
        );
    }

    deletePurgeRequest(connectorId: string, purgeRequestId: string): Observable<DropRequestEntity> {
        return this.httpClient.delete<DropRequestEntity>(
            `${ConnectorService.API}/connectors/${connectorId}/purge-requests/${purgeRequestId}`
        );
    }
}
