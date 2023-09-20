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
import { CreateComponent, CreatePort, DeleteComponent, Snippet, UpdateComponent } from '../state/flow';
import { ComponentType } from '../state/shared';
import { Client } from './client.service';

@Injectable({ providedIn: 'root' })
export class FlowService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private canvasUtils: CanvasUtils,
        private client: Client
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
        return this.canvasUtils.substringAfterFirst(url, ':');
    }

    getFlow(processGroupId: string = 'root'): Observable<any> {
        return this.httpClient.get(`${FlowService.API}/flow/process-groups/${processGroupId}`);
    }

    createFunnel(processGroupId: string = 'root', createFunnel: CreateComponent): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/funnels`, {
            revision: createFunnel.revision,
            component: {
                position: createFunnel.position
            }
        });
    }

    createLabel(processGroupId: string = 'root', createLabel: CreateComponent): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/process-groups/${processGroupId}/labels`, {
            revision: createLabel.revision,
            component: {
                position: createLabel.position
            }
        });
    }

    createPort(processGroupId: string = 'root', createPort: CreatePort): Observable<any> {
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

    updateComponent(updateComponent: UpdateComponent): Observable<any> {
        // return throwError('API Error');
        return this.httpClient.put(this.stripProtocol(updateComponent.uri), updateComponent.payload);
    }

    deleteComponent(deleteComponent: DeleteComponent): Observable<any> {
        // return throwError('API Error');
        const revision: any = this.client.getRevision(deleteComponent.entity);
        return this.httpClient.delete(this.stripProtocol(deleteComponent.uri), { params: revision });
    }

    createSnippet(snippet: Snippet): Observable<any> {
        return this.httpClient.post(`${FlowService.API}/snippets`, { snippet });
    }

    deleteSnippet(snippetId: string): Observable<any> {
        return this.httpClient.delete(`${FlowService.API}/snippets/${snippetId}`);
    }
}
