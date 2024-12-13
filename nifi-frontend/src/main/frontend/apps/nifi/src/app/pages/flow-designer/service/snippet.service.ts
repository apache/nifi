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
import { HttpClient, HttpParams } from '@angular/common/http';
import { Snippet, SnippetComponentRequest } from '../state/flow';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ComponentType } from '@nifi/shared';
import { Client } from '../../../service/client.service';

@Injectable({ providedIn: 'root' })
export class SnippetService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {}

    marshalSnippet(components: SnippetComponentRequest[], processGroupId: string): Snippet {
        return components.reduce(
            (snippet, component) => {
                switch (component.type) {
                    case ComponentType.Processor:
                        snippet.processors[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.InputPort:
                        snippet.inputPorts[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.OutputPort:
                        snippet.outputPorts[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.ProcessGroup:
                        snippet.processGroups[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.RemoteProcessGroup:
                        snippet.remoteProcessGroups[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.Funnel:
                        snippet.funnels[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.Label:
                        snippet.labels[component.id] = this.client.getRevision(component.entity);
                        break;
                    case ComponentType.Connection:
                        snippet.connections[component.id] = this.client.getRevision(component.entity);
                        break;
                }
                return snippet;
            },
            {
                parentGroupId: processGroupId,
                processors: {},
                funnels: {},
                inputPorts: {},
                outputPorts: {},
                remoteProcessGroups: {},
                processGroups: {},
                connections: {},
                labels: {}
            } as Snippet
        );
    }

    createSnippet(snippet: Snippet): Observable<any> {
        return this.httpClient.post(`${SnippetService.API}/snippets`, {
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            snippet
        });
    }

    moveSnippet(snippetId: string, groupId: string): Observable<any> {
        const payload: any = {
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            snippet: {
                id: snippetId,
                parentGroupId: groupId
            }
        };
        return this.httpClient.put(`${SnippetService.API}/snippets/${snippetId}`, payload);
    }

    deleteSnippet(snippetId: string): Observable<any> {
        const params = new HttpParams({
            fromObject: {
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(`${SnippetService.API}/snippets/${snippetId}`, { params });
    }
}
