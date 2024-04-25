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
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { ClusterListingResponse, ClusterNodeEntity } from '../state/cluster-listing';

@Injectable({ providedIn: 'root' })
export class ClusterService {
    private static readonly API = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getClusterListing(): Observable<ClusterListingResponse> {
        return this.httpClient.get(`${ClusterService.API}/controller/cluster`) as Observable<ClusterListingResponse>;
    }

    disconnectNode(nodeId: string): Observable<ClusterNodeEntity> {
        return this.httpClient.put(`${ClusterService.API}/controller/cluster/nodes/${nodeId}`, {
            node: {
                nodeId,
                status: 'DISCONNECTING'
            }
        }) as Observable<ClusterNodeEntity>;
    }

    connectNode(nodeId: string): Observable<ClusterNodeEntity> {
        return this.httpClient.put(`${ClusterService.API}/controller/cluster/nodes/${nodeId}`, {
            node: {
                nodeId,
                status: 'CONNECTING'
            }
        }) as Observable<ClusterNodeEntity>;
    }

    offloadNode(nodeId: string): Observable<ClusterNodeEntity> {
        return this.httpClient.put(`${ClusterService.API}/controller/cluster/nodes/${nodeId}`, {
            node: {
                nodeId,
                status: 'OFFLOADING'
            }
        }) as Observable<ClusterNodeEntity>;
    }

    removeNode(nodeId: string) {
        return this.httpClient.delete(`${ClusterService.API}/controller/cluster/nodes/${nodeId}`);
    }
}
