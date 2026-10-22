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
import { HttpClient } from '@angular/common/http';
import { Observable, switchMap } from 'rxjs';
import { UpdateComponentRequest } from '../../../state/shared';
import { CreateConnection } from '../../../state/flow-shared';
import { ComponentType, ConnectionEntity, NiFiCommon, RevisionRequest } from '@nifi/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

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

    moveComponents(
        components: ConnectorSnippetComponent[],
        processGroupId: string,
        targetGroupId: string
    ): Observable<unknown> {
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
        const disconnectedNodeAcknowledged = this.clusterConnectionService.isDisconnectionAcknowledged();

        return this.httpClient
            .post<{ snippet: { id: string } }>(`${ConnectorEditService.API}/snippets`, {
                disconnectedNodeAcknowledged,
                snippet
            })
            .pipe(
                switchMap((response) =>
                    this.httpClient.put(`${ConnectorEditService.API}/snippets/${response.snippet.id}`, {
                        disconnectedNodeAcknowledged,
                        snippet: {
                            id: response.snippet.id,
                            parentGroupId: targetGroupId
                        }
                    })
                )
            );
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
