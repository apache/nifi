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
import { Observable, throwError } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { CanvasUtils } from './canvas-utils.service';
import {
    CreateComponentRequest,
    CreateConnection,
    CreatePortRequest,
    CreateProcessGroupRequest,
    CreateProcessorRequest,
    DeleteComponentRequest,
    ReplayLastProvenanceEventRequest,
    Snippet,
    UpdateComponentRequest,
    UploadProcessGroupRequest
} from '../state/flow';
import { ComponentType } from '../../../state/shared';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';

@Injectable({ providedIn: 'root' })
export class FlowService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private canvasUtils: CanvasUtils,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    /**
     * The NiFi model contain the url for each component. That URL is an absolute URL. Angular CSRF handling
     * does not work on absolute URLs, so we need to strip off the proto for the request header to be added.
     *
     * https://stackoverflow.com/a/59586462
     *
     * @param url
     * @private
     */
    private stripProtocol(url: string): string {
        return this.nifiCommon.substringAfterFirst(url, ':');
    }

    getFlow(processGroupId: string = 'root'): Observable<any> {
        // TODO - support uiOnly... this would mean that we need to load the entire resource prior to editing
        return this.httpClient.get(`${FlowService.API}/flow/process-groups/${processGroupId}`);
    }

    getProcessGroupStatus(processGroupId: string = 'root', recursive: boolean = false): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/process-groups/${processGroupId}/status`, {
            params: { recursive: recursive }
        });
    }

    getFlowStatus(): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/status`);
    }

    getClusterSummary(): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/cluster/summary`);
    }

    getControllerBulletins(): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/controller/bulletins`);
    }

    getParameterContexts(): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/parameter-contexts`);
    }

    getParameterContext(id: string): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/parameter-contexts/${id}`, {
            params: { includeInheritedParameters: true }
        });
    }

    getProcessor(id: string): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/processors/${id}`);
    }

    getInputPort(id: string): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/input-ports/${id}`);
    }

    getRemoteProcessGroup(id: string): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/remote-process-groups/${id}`);
    }

    getConnection(id: string): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/connections/${id}`);
    }

    getProcessGroup(id: string): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/process-groups/${id}`);
    }

    createFunnel(processGroupId: string = 'root', createFunnel: CreateComponentRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/funnels`, {
            revision: createFunnel.revision,
            component: {
                position: createFunnel.position
            }
        });
    }

    createLabel(processGroupId: string = 'root', createLabel: CreateComponentRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/labels`, {
            revision: createLabel.revision,
            component: {
                position: createLabel.position
            }
        });
    }

    createProcessor(processGroupId: string = 'root', createProcessor: CreateProcessorRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/processors`, {
            revision: createProcessor.revision,
            component: {
                position: createProcessor.position,
                type: createProcessor.processorType,
                bundle: createProcessor.processorBundle
            }
        });
    }

    createConnection(processGroupId: string = 'root', createConnection: CreateConnection): Observable<any> {
        return this.httpClient.post(
            `${FlowService.API}/process-groups/${processGroupId}/connections`,
            createConnection.payload
        );
    }

    createProcessGroup(
        processGroupId: string = 'root',
        createProcessGroup: CreateProcessGroupRequest
    ): Observable<any> {
        const payload: any = {
            revision: createProcessGroup.revision,
            component: {
                position: createProcessGroup.position,
                name: createProcessGroup.name
            }
        };

        if (createProcessGroup.parameterContextId) {
            payload.component.parameterContext = {
                id: createProcessGroup.parameterContextId
            };
        }

        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/process-groups`, payload);
    }

    uploadProcessGroup(
        processGroupId: string = 'root',
        uploadProcessGroup: UploadProcessGroupRequest
    ): Observable<any> {
        const payload = new FormData();
        payload.append('id', processGroupId);
        payload.append('groupName', uploadProcessGroup.name);
        payload.append('positionX', uploadProcessGroup.position.x.toString());
        payload.append('positionY', uploadProcessGroup.position.y.toString());
        payload.append('clientId', uploadProcessGroup.revision.clientId);
        payload.append('file', uploadProcessGroup.flowDefinition);

        return this.httpClient.post(
            `${FlowService.API}/process-groups/${processGroupId}/process-groups/upload`,
            payload
        );
    }

    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<any> {
        const params: any = {
            propertyName,
            sensitive
        };
        return this.httpClient.get(`${FlowService.API}/processors/${id}/descriptors`, {
            params
        });
    }

    createPort(processGroupId: string = 'root', createPort: CreatePortRequest): Observable<any> {
        const portType: string = ComponentType.InputPort == createPort.type ? 'input-ports' : 'output-ports';
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/${portType}`, {
            revision: createPort.revision,
            component: {
                position: createPort.position,
                name: createPort.name,
                allowRemoteAccess: createPort.allowRemoteAccess
            }
        });
    }

    updateComponent(updateComponent: UpdateComponentRequest): Observable<any> {
        // return throwError('API Error');
        return this.httpClient.put(this.stripProtocol(updateComponent.uri), updateComponent.payload);
    }

    deleteComponent(deleteComponent: DeleteComponentRequest): Observable<any> {
        // return throwError('API Error');
        const revision: any = this.client.getRevision(deleteComponent.entity);
        return this.httpClient.delete(this.stripProtocol(deleteComponent.uri), { params: revision });
    }

    createSnippet(snippet: Snippet): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/snippets`, { snippet });
    }

    moveSnippet(snippetId: string, groupId: string): Observable<any> {
        const payload: any = {
            // 'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
            snippet: {
                id: snippetId,
                parentGroupId: groupId
            }
        };
        return this.httpClient.put(`${FlowService.API}/snippets/${snippetId}`, payload);
    }

    deleteSnippet(snippetId: string): Observable<any> {
        return this.httpClient.delete(`${FlowService.API}/snippets/${snippetId}`);
    }

    replayLastProvenanceEvent(request: ReplayLastProvenanceEventRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/provenance-events/latest/replays`, request);
    }
}
