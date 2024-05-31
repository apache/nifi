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

export const lineageFeatureKey = 'lineageGraph';

export interface LineageRequest {
    eventId?: string;
    lineageRequestType: 'PARENTS' | 'CHILDREN' | 'FLOWFILE';
    uuid?: string;
    clusterNodeId?: string;
}

export interface LineageQueryResponse {
    lineage: Lineage;
}

export interface LineageNode {
    id: string;
    flowFileUuid: string;
    parentUuids: string[];
    childUuids: string[];
    clusterNodeIdentifier: string;
    type: string;
    eventType: string;
    millis: number;
    timestamp: string;
}

export interface LineageLink {
    sourceId: string;
    targetId: string;
    flowFileUuid: string;
    timestamp: string;
    millis: number;
}

export interface LineageResults {
    errors?: string[];
    nodes: LineageNode[];
    links: LineageLink[];
}

export interface Lineage {
    id: string;
    uri: string;
    submissionTime: string;
    expiration: string;
    percentCompleted: number;
    finished: boolean;
    request: LineageRequest;
    results: LineageResults;
}

export interface LineageState {
    activeLineage: Lineage | null;
    completedLineage: Lineage;
    status: 'pending' | 'loading' | 'error' | 'success';
}
