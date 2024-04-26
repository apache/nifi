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

export const clusterListingFeatureKey = 'clusterListing';

export interface ClusterListingState {
    nodes: ClusterNode[];
    loadedTimestamp: string;
    saving: boolean;
    status: 'pending' | 'loading' | 'success';
}

export interface ClusterNodeEntity {
    node: ClusterNode;
}

export interface ClusterNode {
    nodeId: string;
    address: string;
    apiPort: number;
    status: string;
    roles: string[];
    events: ClusterNodeEvent[];
    activeThreadCount?: number;
    queued?: string;
    bytesQueued?: number;
    flowFilesQueued?: number;
    heartbeat?: string;
    connectionRequested?: string;
    nodeStartTime?: string;
}

export interface ClusterNodeEvent {
    timestamp: string;
    category: string;
    message: string;
}

export interface ClusterListingResponse {
    cluster: ClusterListingEntity;
}

export interface ClusterListingEntity {
    nodes: ClusterNode[];
    generated: string;
}

export interface SelectClusterNodeRequest {
    id: string;
    repository?: string;
}
