/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { ComponentType } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class ComponentClusterStatusService {
    private static readonly API = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getClusterStatus(componentId: string, componentType: ComponentType, nodewise: boolean = true): Observable<any> {
        let componentPath: string;
        switch (componentType) {
            case ComponentType.Processor:
                componentPath = 'processors';
                break;
            case ComponentType.ProcessGroup:
                componentPath = 'process-groups';
                break;
            case ComponentType.Connection:
                componentPath = 'connections';
                break;
            case ComponentType.RemoteProcessGroup:
                componentPath = 'remote-process-groups';
                break;
            case ComponentType.InputPort:
                componentPath = 'input-ports';
                break;
            case ComponentType.OutputPort:
                componentPath = 'output-ports';
                break;
            default:
                throw 'Unsupported component type';
        }

        let params: HttpParams = new HttpParams();
        if (nodewise) {
            params = params.set('nodewise', true);
        }

        return this.httpClient.get(
            `${ComponentClusterStatusService.API}/flow/${componentPath}/${encodeURIComponent(componentId)}/status`,
            { params }
        );
    }
}
