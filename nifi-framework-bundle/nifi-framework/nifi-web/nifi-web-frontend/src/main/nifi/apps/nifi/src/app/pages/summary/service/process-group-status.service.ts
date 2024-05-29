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
import { LoadSummaryRequest } from '../state/summary-listing';

@Injectable({ providedIn: 'root' })
export class ProcessGroupStatusService {
    private static readonly API: string = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getProcessGroupsStatus(request: LoadSummaryRequest): Observable<any> {
        let params: HttpParams = new HttpParams();
        if (request?.recursive) {
            params = params.set('recursive', true);
        }
        if (request?.clusterNodeId && request?.clusterNodeId !== 'All') {
            params = params.set('clusterNodeId', request.clusterNodeId);
        }
        return this.httpClient.get(`${ProcessGroupStatusService.API}/flow/process-groups/root/status`, { params });
    }
}
