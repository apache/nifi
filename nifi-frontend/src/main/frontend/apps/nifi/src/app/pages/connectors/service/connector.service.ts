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
import { defer, Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { HttpClient } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ChangeConnectorVersionRequest, ConnectorsResponse, CreateConnectorRequest } from '../state';
import { ConnectorEntity, ControllerServiceEntity, ProcessGroupEntity, safeApiPath } from '@nifi/shared';
import { ParameterContextEntity, ProcessGroupFlowEntity, SearchResultsEntity } from '../../../state/shared';
import { DropRequestEntity } from '../../../state/empty-queue';
import type { DownloadConnectorFlowRequest } from '../state/connector-canvas/connector-canvas.actions';

@Injectable({ providedIn: 'root' })
export class ConnectorService {
    private httpClient = inject(HttpClient);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    getConnectors(): Observable<ConnectorsResponse> {
        return this.httpClient.get<ConnectorsResponse>(`${ConnectorService.API}/flow/connectors`);
    }

    getConnector(connectorId: string): Observable<ConnectorEntity> {
        return defer(() =>
            this.httpClient.get<ConnectorEntity>(`${ConnectorService.API}/connectors/${safeApiPath(connectorId)}`)
        );
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
        return defer(() =>
            this.httpClient.put<ConnectorEntity>(`${ConnectorService.API}/connectors/${safeApiPath(connector.id)}`, {
                revision: this.client.getRevision(connector),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: connector.component,
                id: connector.id
            })
        );
    }

    changeConnectorVersion(request: ChangeConnectorVersionRequest): Observable<ConnectorEntity> {
        return defer(() =>
            this.httpClient.put<ConnectorEntity>(`${ConnectorService.API}/connectors/${safeApiPath(request.id)}`, {
                revision: this.client.getRevision({ revision: request.payload.revision }),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: request.payload.component,
                id: request.id
            })
        );
    }

    deleteConnector(connector: ConnectorEntity): Observable<ConnectorEntity> {
        return defer(() => {
            const revision = this.client.getRevision(connector);
            const params: Record<string, string | number | boolean> = {
                ...revision,
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            };
            return this.httpClient.delete<ConnectorEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connector.id)}`,
                { params }
            );
        });
    }

    updateConnectorRunStatus(connector: ConnectorEntity, runStatus: string): Observable<ConnectorEntity> {
        return defer(() =>
            this.httpClient.put<ConnectorEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connector.id)}/run-status`,
                {
                    revision: this.client.getRevision(connector),
                    state: runStatus,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
                }
            )
        );
    }

    discardConnectorWorkingConfiguration(connector: ConnectorEntity): Observable<ConnectorEntity> {
        return defer(() => {
            const revision = this.client.getRevision(connector);
            const params: Record<string, string | number | boolean> = {
                ...revision,
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            };
            return this.httpClient.delete<ConnectorEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connector.id)}/working-configuration`,
                { params }
            );
        });
    }

    drainConnector(connector: ConnectorEntity): Observable<ConnectorEntity> {
        return defer(() =>
            this.httpClient.post<ConnectorEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connector.id)}/drain`,
                {
                    revision: this.client.getRevision(connector),
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
                }
            )
        );
    }

    cancelConnectorDrain(connector: ConnectorEntity): Observable<ConnectorEntity> {
        return defer(() => {
            const revision = this.client.getRevision(connector);
            const params: Record<string, string | number | boolean> = {
                ...revision,
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            };
            return this.httpClient.delete<ConnectorEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connector.id)}/drain`,
                { params }
            );
        });
    }

    getConnectorFlow(connectorId: string, processGroupId: string): Observable<ProcessGroupFlowEntity> {
        return defer(() =>
            this.httpClient.get<ProcessGroupFlowEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/flow/process-groups/${safeApiPath(processGroupId)}`
            )
        );
    }

    getProcessGroup(connectorId: string, processGroupId: string): Observable<ProcessGroupEntity> {
        return defer(() =>
            this.httpClient.get<ProcessGroupEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/process-groups/${safeApiPath(processGroupId)}`
            )
        );
    }

    downloadFlow(request: DownloadConnectorFlowRequest): void {
        window.open(
            `${ConnectorService.API}/connectors/${safeApiPath(request.connectorId)}/process-groups/${safeApiPath(request.processGroupId)}/download?includeReferencedServices=${request.includeReferencedServices}`,
            '_blank',
            'noreferrer'
        );
    }

    getConnectorControllerServices(connectorId: string, processGroupId: string): Observable<any> {
        return defer(() =>
            this.httpClient.get(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/flow/process-groups/${safeApiPath(processGroupId)}/controller-services`
            )
        );
    }

    getControllerService(connectorId: string, controllerServiceId: string): Observable<ControllerServiceEntity> {
        return defer(() => {
            const params: Record<string, boolean> = { uiOnly: true };
            return this.httpClient.get<ControllerServiceEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/controller-services/${safeApiPath(controllerServiceId)}`,
                { params }
            );
        });
    }

    getConnectorParameterContext(
        connectorId: string,
        processGroupId: string
    ): Observable<ParameterContextEntity | null> {
        return defer(() =>
            this.httpClient
                .get<ParameterContextEntity>(
                    `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/flow/process-groups/${safeApiPath(processGroupId)}/parameter-context`,
                    { observe: 'response' }
                )
                .pipe(map((response) => (response.status === 204 ? null : response.body)))
        );
    }

    searchConnector(connectorId: string, query: string): Observable<SearchResultsEntity> {
        return defer(() =>
            this.httpClient.get<SearchResultsEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/search-results`,
                { params: { q: query } }
            )
        );
    }

    // ========================================================================================
    // Purge Methods
    // ========================================================================================

    createPurgeRequest(connectorId: string): Observable<DropRequestEntity> {
        return defer(() =>
            this.httpClient.post<DropRequestEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/purge-requests`,
                {}
            )
        );
    }

    getPurgeRequest(connectorId: string, purgeRequestId: string): Observable<DropRequestEntity> {
        return defer(() =>
            this.httpClient.get<DropRequestEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/purge-requests/${safeApiPath(purgeRequestId)}`
            )
        );
    }

    deletePurgeRequest(connectorId: string, purgeRequestId: string): Observable<DropRequestEntity> {
        return defer(() =>
            this.httpClient.delete<DropRequestEntity>(
                `${ConnectorService.API}/connectors/${safeApiPath(connectorId)}/purge-requests/${safeApiPath(purgeRequestId)}`
            )
        );
    }
}
