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

import { inject, Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { defer, map, Observable, switchMap } from 'rxjs';
import { UpdateComponentRequest } from '../../../state/shared';
import {
    CreateComponentRequest,
    CreateConnection,
    CreateLabelRequest,
    CreatePortRequest,
    CreateProcessGroupRequest,
    CreateProcessorRequest,
    CreateRemoteProcessGroupRequest,
    UploadProcessGroupRequest
} from '../../../state/flow-shared';
import {
    ComponentType,
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    NiFiCommon,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    RevisionRequest,
    safeApiPath
} from '@nifi/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import {
    ConnectorCanvasComponentEntity,
    ConnectorCanvasCreatedComponent,
    ConnectorCanvasCreateRequest,
    ConnectorCanvasDeleteRequest
} from '../state/connector-canvas';

export type ConnectorComponentUpdateRequest = Pick<UpdateComponentRequest, 'id' | 'type' | 'payload'>;

export interface ConnectorSnippetComponent {
    id: string;
    type: ComponentType;
    revision: RevisionRequest;
}

@Injectable({ providedIn: 'root' })
export class ConnectorEditService {
    private static readonly API = '../nifi-api';

    private readonly httpClient = inject(HttpClient);
    private readonly nifiCommon = inject(NiFiCommon);
    private readonly clusterConnectionService = inject(ClusterConnectionService);

    updateComponent<T>(request: ConnectorComponentUpdateRequest): Observable<T> {
        const path = this.nifiCommon.getComponentTypeApiPath(request.type);
        return this.httpClient.put<T>(`${ConnectorEditService.API}/${path}/${request.id}`, request.payload);
    }

    createConnection(processGroupId: string, request: CreateConnection): Observable<ConnectionEntity> {
        return this.httpClient.post<ConnectionEntity>(
            `${ConnectorEditService.API}/process-groups/${processGroupId}/connections`,
            request.payload
        );
    }

    /**
     * Create a component of the request's type and return it tagged with that type.
     */
    createComponent(
        processGroupId: string,
        request: ConnectorCanvasCreateRequest
    ): Observable<ConnectorCanvasCreatedComponent> {
        switch (request.type) {
            case ComponentType.Processor:
                return this.createProcessor(processGroupId, request).pipe(
                    map((payload) => ({ type: request.type, payload }))
                );
            case ComponentType.InputPort:
            case ComponentType.OutputPort:
                return this.createPort(processGroupId, request).pipe(
                    map((payload) => ({ type: request.type, payload }))
                );
            case ComponentType.ProcessGroup:
                return this.createProcessGroup(processGroupId, request).pipe(
                    map((payload) => ({ type: request.type, payload }))
                );
            case ComponentType.RemoteProcessGroup:
                return this.createRemoteProcessGroup(processGroupId, request).pipe(
                    map((payload) => ({ type: request.type, payload }))
                );
            case ComponentType.Funnel:
                return this.createFunnel(processGroupId, request).pipe(
                    map((payload) => ({ type: request.type, payload }))
                );
            case ComponentType.Label:
                return this.createLabel(processGroupId, request).pipe(
                    map((payload) => ({ type: request.type, payload }))
                );
        }
    }

    createProcessor(processGroupId: string, request: CreateProcessorRequest): Observable<ProcessorEntity> {
        return defer(() =>
            this.httpClient.post<ProcessorEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/processors`,
                {
                    revision: request.revision,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    component: {
                        position: request.position,
                        type: request.processorType,
                        bundle: request.processorBundle
                    }
                }
            )
        );
    }

    createPort(processGroupId: string, request: CreatePortRequest): Observable<PortEntity> {
        const portType = request.type === ComponentType.InputPort ? 'input-ports' : 'output-ports';
        return defer(() =>
            this.httpClient.post<PortEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/${portType}`,
                {
                    revision: request.revision,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    component: {
                        position: request.position,
                        name: request.name,
                        allowRemoteAccess: request.allowRemoteAccess
                    }
                }
            )
        );
    }

    createProcessGroup(processGroupId: string, request: CreateProcessGroupRequest): Observable<ProcessGroupEntity> {
        return defer(() =>
            this.httpClient.post<ProcessGroupEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/process-groups`,
                {
                    revision: request.revision,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    component: {
                        position: request.position,
                        name: request.name,
                        ...(request.parameterContextId ? { parameterContext: { id: request.parameterContextId } } : {})
                    }
                }
            )
        );
    }

    createRemoteProcessGroup(
        processGroupId: string,
        request: CreateRemoteProcessGroupRequest
    ): Observable<RemoteProcessGroupEntity> {
        return defer(() =>
            this.httpClient.post<RemoteProcessGroupEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/remote-process-groups`,
                {
                    revision: request.revision,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    component: {
                        position: request.position,
                        targetUris: request.targetUris,
                        transportProtocol: request.transportProtocol,
                        localNetworkInterface: request.localNetworkInterface,
                        proxyHost: request.proxyHost,
                        proxyPort: request.proxyPort,
                        proxyUser: request.proxyUser,
                        proxyPassword: request.proxyPassword,
                        communicationsTimeout: request.communicationsTimeout,
                        yieldDuration: request.yieldDuration
                    }
                }
            )
        );
    }

    createFunnel(processGroupId: string, request: CreateComponentRequest): Observable<FunnelEntity> {
        return defer(() =>
            this.httpClient.post<FunnelEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/funnels`,
                {
                    revision: request.revision,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    component: {
                        position: request.position
                    }
                }
            )
        );
    }

    createLabel(processGroupId: string, request: CreateLabelRequest): Observable<LabelEntity> {
        return defer(() =>
            this.httpClient.post<LabelEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/labels`,
                {
                    revision: request.revision,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    component: {
                        position: request.position,
                        zIndex: request.zIndex
                    }
                }
            )
        );
    }

    uploadProcessGroup(processGroupId: string, request: UploadProcessGroupRequest): Observable<ProcessGroupEntity> {
        return defer(() => {
            const payload = new FormData();
            payload.append('id', processGroupId);
            payload.append('groupName', request.name);
            payload.append('positionX', request.position.x.toString());
            payload.append('positionY', request.position.y.toString());
            payload.append('clientId', request.revision.clientId ?? '');
            payload.append(
                'disconnectedNodeAcknowledged',
                String(this.clusterConnectionService.isDisconnectionAcknowledged())
            );
            payload.append('file', request.flowDefinition);

            return this.httpClient.post<ProcessGroupEntity>(
                `${ConnectorEditService.API}/process-groups/${safeApiPath(processGroupId)}/process-groups/upload`,
                payload
            );
        });
    }

    getComponent(type: ComponentType, id: string): Observable<ConnectorCanvasComponentEntity> {
        return defer(() =>
            this.httpClient.get<ConnectorCanvasComponentEntity>(
                `${ConnectorEditService.API}/${this.nifiCommon.getComponentTypeApiPath(type)}/${safeApiPath(id)}`
            )
        );
    }

    deleteComponent(component: ConnectorCanvasDeleteRequest): Observable<unknown> {
        return defer(() => {
            const path = this.nifiCommon.getComponentTypeApiPath(component.type);
            const params = new HttpParams({
                fromObject: {
                    version: component.revision.version,
                    clientId: component.revision.clientId ?? '',
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
                }
            });
            return this.httpClient.delete(`${ConnectorEditService.API}/${path}/${safeApiPath(component.id)}`, {
                params
            });
        });
    }

    /**
     * Delete several components atomically by creating and then deleting a snippet.
     */
    deleteComponents(components: ConnectorSnippetComponent[], processGroupId: string): Observable<unknown> {
        const disconnectedNodeAcknowledged = this.clusterConnectionService.isDisconnectionAcknowledged();

        return this.createSnippet(components, processGroupId).pipe(
            switchMap((response) =>
                defer(() =>
                    this.httpClient.delete(`${ConnectorEditService.API}/snippets/${safeApiPath(response.snippet.id)}`, {
                        params: new HttpParams({ fromObject: { disconnectedNodeAcknowledged } })
                    })
                )
            )
        );
    }

    moveComponents(
        components: ConnectorSnippetComponent[],
        processGroupId: string,
        targetGroupId: string
    ): Observable<unknown> {
        const disconnectedNodeAcknowledged = this.clusterConnectionService.isDisconnectionAcknowledged();

        return this.createSnippet(components, processGroupId).pipe(
            switchMap((response) =>
                this.httpClient.put(`${ConnectorEditService.API}/snippets/${safeApiPath(response.snippet.id)}`, {
                    disconnectedNodeAcknowledged,
                    snippet: {
                        id: response.snippet.id,
                        parentGroupId: targetGroupId
                    }
                })
            )
        );
    }

    private createSnippet(
        components: ConnectorSnippetComponent[],
        processGroupId: string
    ): Observable<{ snippet: { id: string } }> {
        const snippet = {
            parentGroupId: processGroupId,
            processors: this.revisionsFor(components, ComponentType.Processor),
            inputPorts: this.revisionsFor(components, ComponentType.InputPort),
            outputPorts: this.revisionsFor(components, ComponentType.OutputPort),
            processGroups: this.revisionsFor(components, ComponentType.ProcessGroup),
            remoteProcessGroups: this.revisionsFor(components, ComponentType.RemoteProcessGroup),
            funnels: this.revisionsFor(components, ComponentType.Funnel),
            labels: this.revisionsFor(components, ComponentType.Label),
            connections: this.revisionsFor(components, ComponentType.Connection)
        };

        return this.httpClient.post<{ snippet: { id: string } }>(`${ConnectorEditService.API}/snippets`, {
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            snippet
        });
    }

    private revisionsFor(
        components: ConnectorSnippetComponent[],
        type: ComponentType
    ): Record<string, RevisionRequest> {
        return components
            .filter((component) => component.type === type)
            .reduce<Record<string, RevisionRequest>>((revisions, component) => {
                revisions[component.id] = component.revision;
                return revisions;
            }, {});
    }
}
