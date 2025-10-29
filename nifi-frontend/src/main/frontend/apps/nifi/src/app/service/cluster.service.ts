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
import { Observable } from 'rxjs';
import { HttpClient, HttpParams } from '@angular/common/http';
import { ClusterSearchResults } from '../state/cluster-summary';

@Injectable({ providedIn: 'root' })
export class ClusterService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-api';

    getClusterSummary(): Observable<any> {
        return this.httpClient.get(`${ClusterService.API}/flow/cluster/summary`);
    }

    searchCluster(q?: string): Observable<ClusterSearchResults> {
        let params = new HttpParams();
        if (q) {
            params = params.set('q', q);
        }

        return this.httpClient.get<ClusterSearchResults>(`${ClusterService.API}/flow/cluster/search-results`, {
            params
        });
    }
}
