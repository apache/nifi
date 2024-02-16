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
import {
    ComponentRunStatusRequest,
    CreateComponentRequest,
    CreateConnection,
    CreatePortRequest,
    CreateProcessGroupRequest,
    CreateProcessorRequest,
    CreateRemoteProcessGroupRequest,
    DeleteComponentRequest,
    GoToRemoteProcessGroupRequest,
    ProcessGroupRunStatusRequest,
    ReplayLastProvenanceEventRequest,
    RunOnceRequest,
    Snippet,
    StartComponentRequest,
    StartProcessGroupRequest,
    StopComponentRequest,
    StopProcessGroupRequest,
    UpdateComponentRequest,
    UploadProcessGroupRequest
} from '../state/flow';
import { ComponentType, PropertyDescriptorRetriever } from '../../../state/shared';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';

@Injectable({ providedIn: 'root' })
export class FlowService implements PropertyDescriptorRetriever {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getFlow(processGroupId = 'root'): Observable<any> {
        // TODO - support uiOnly... this would mean that we need to load the entire resource prior to editing
        return this.httpClient.get(`${FlowService.API}/flow/process-groups/${processGroupId}`);
    }

    getProcessGroupStatus(processGroupId = 'root', recursive = false): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/process-groups/${processGroupId}/status`, {
            params: { recursive: recursive }
        });
    }

    getFlowStatus(): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/status`);
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

    createFunnel(processGroupId = 'root', createFunnel: CreateComponentRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/funnels`, {
            revision: createFunnel.revision,
            component: {
                position: createFunnel.position
            }
        });
    }

    createLabel(processGroupId = 'root', createLabel: CreateComponentRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/labels`, {
            revision: createLabel.revision,
            component: {
                position: createLabel.position
            }
        });
    }

    goToRemoteProcessGroup(goToRemoteProcessGroupRequest: GoToRemoteProcessGroupRequest) {
        window.open(encodeURI(goToRemoteProcessGroupRequest.uri));
    }

    createProcessor(processGroupId = 'root', createProcessor: CreateProcessorRequest): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/processors`, {
            revision: createProcessor.revision,
            component: {
                position: createProcessor.position,
                type: createProcessor.processorType,
                bundle: createProcessor.processorBundle
            }
        });
    }

    createConnection(processGroupId = 'root', createConnection: CreateConnection): Observable<any> {
        return this.httpClient.post(
            `${FlowService.API}/process-groups/${processGroupId}/connections`,
            createConnection.payload
        );
    }

    createProcessGroup(processGroupId = 'root', createProcessGroup: CreateProcessGroupRequest): Observable<any> {
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

    createRemoteProcessGroup(
        processGroupId = 'root',
        createRemoteProcessGroup: CreateRemoteProcessGroupRequest
    ): Observable<any> {
        const payload: any = {
            revision: createRemoteProcessGroup.revision,
            component: {
                position: createRemoteProcessGroup.position,
                targetUris: createRemoteProcessGroup.targetUris,
                transportProtocol: createRemoteProcessGroup.transportProtocol,
                localNetworkInterface: createRemoteProcessGroup.localNetworkInterface,
                proxyHost: createRemoteProcessGroup.proxyHost,
                proxyPort: createRemoteProcessGroup.proxyPort,
                proxyUser: createRemoteProcessGroup.proxyUser,
                proxyPassword: createRemoteProcessGroup.proxyPassword,
                communicationsTimeout: createRemoteProcessGroup.communicationsTimeout,
                yieldDuration: createRemoteProcessGroup.yieldDuration
            }
        };

        return this.httpClient.post(
            `${FlowService.API}/process-groups/${processGroupId}/remote-process-groups`,
            payload
        );
    }

    uploadProcessGroup(processGroupId = 'root', uploadProcessGroup: UploadProcessGroupRequest): Observable<any> {
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

    createPort(processGroupId = 'root', createPort: CreatePortRequest): Observable<any> {
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
        return this.httpClient.put(this.nifiCommon.stripProtocol(updateComponent.uri), updateComponent.payload);
    }

    deleteComponent(deleteComponent: DeleteComponentRequest): Observable<any> {
        const revision: any = this.client.getRevision(deleteComponent.entity);
        return this.httpClient.delete(this.nifiCommon.stripProtocol(deleteComponent.uri), { params: revision });
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

    runOnce(request: RunOnceRequest): Observable<any> {
        const startRequest: ComponentRunStatusRequest = {
            revision: request.revision,
            disconnectedNodeAcknowledged: false,
            state: 'RUN_ONCE'
        };
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(request.uri)}/run-status`, startRequest);
    }

    startComponent(request: StartComponentRequest): Observable<any> {
        const startRequest: ComponentRunStatusRequest = {
            revision: request.revision,
            disconnectedNodeAcknowledged: false,
            state: request.type === ComponentType.RemoteProcessGroup ? 'TRANSMITTING' : 'RUNNING'
        };
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(request.uri)}/run-status`, startRequest);
    }

    stopComponent(request: StopComponentRequest): Observable<any> {
        const stopRequest: ComponentRunStatusRequest = {
            revision: request.revision,
            disconnectedNodeAcknowledged: false,
            state: 'STOPPED'
        };
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(request.uri)}/run-status`, stopRequest);
    }

    startProcessGroup(request: StartProcessGroupRequest): Observable<any> {
        const startRequest: ProcessGroupRunStatusRequest = {
            id: request.id,
            disconnectedNodeAcknowledged: false,
            state: 'RUNNING'
        };
        return this.httpClient.put(`${FlowService.API}/flow/process-groups/${request.id}`, startRequest);
    }

    startRemoteProcessGroupsInProcessGroup(request: StartProcessGroupRequest): Observable<any> {
        const startRequest = {
            disconnectedNodeAcknowledged: false,
            state: 'TRANSMITTING'
        };
        return this.httpClient.put(
            `${FlowService.API}/remote-process-groups/process-group/${request.id}/run-status`,
            startRequest
        );
    }

    stopProcessGroup(request: StopProcessGroupRequest): Observable<any> {
        const stopRequest: ProcessGroupRunStatusRequest = {
            id: request.id,
            disconnectedNodeAcknowledged: false,
            state: 'STOPPED'
        };
        return this.httpClient.put(`${FlowService.API}/flow/process-groups/${request.id}`, stopRequest);
    }

    stopRemoteProcessGroupsInProcessGroup(request: StopProcessGroupRequest): Observable<any> {
        const stopRequest = {
            disconnectedNodeAcknowledged: false,
            state: 'STOPPED'
        };
        return this.httpClient.put(
            `${FlowService.API}/remote-process-groups/process-group/${request.id}/run-status`,
            stopRequest
        );
    }
}
