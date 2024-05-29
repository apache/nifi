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
import { ImportFromRegistryRequest } from '../state/flow';

@Injectable({ providedIn: 'root' })
export class RegistryService {
    private static readonly API: string = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getRegistryClients(): Observable<any> {
        return this.httpClient.get(`${RegistryService.API}/flow/registries`);
    }

    getBranches(registryId: string): Observable<any> {
        return this.httpClient.get(`${RegistryService.API}/flow/registries/${registryId}/branches`);
    }

    getBuckets(registryId: string, branch?: string): Observable<any> {
        const params: HttpParams = new HttpParams();
        if (branch) {
            params.set('branch', branch);
        }
        return this.httpClient.get(`${RegistryService.API}/flow/registries/${registryId}/buckets`, { params });
    }

    getFlows(registryId: string, bucketId: string, branch?: string): Observable<any> {
        const params: HttpParams = new HttpParams();
        if (branch) {
            params.set('branch', branch);
        }
        return this.httpClient.get(`${RegistryService.API}/flow/registries/${registryId}/buckets/${bucketId}/flows`, {
            params
        });
    }

    getFlowVersions(registryId: string, bucketId: string, flowId: string, branch?: string): Observable<any> {
        const params: HttpParams = new HttpParams();
        if (branch) {
            params.set('branch', branch);
        }
        return this.httpClient.get(
            `${RegistryService.API}/flow/registries/${registryId}/buckets/${bucketId}/flows/${flowId}/versions`,
            { params }
        );
    }

    importFromRegistry(processGroupId: string, request: ImportFromRegistryRequest): Observable<any> {
        return this.httpClient.post(
            `${RegistryService.API}/process-groups/${processGroupId}/process-groups`,
            request.payload,
            {
                params: {
                    parameterContextHandlingStrategy: request.keepExistingParameterContext ? 'KEEP_EXISTING' : 'REPLACE'
                }
            }
        );
    }
}
