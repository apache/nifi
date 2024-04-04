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

export const flowConfigurationHistoryListingFeatureKey = 'listing';

// Returned from API call to /nifi-api/flow/history
export interface HistoryEntity {
    history: History;
}

export interface History {
    total: number;
    lastRefreshed: string;
    actions: ActionEntity[];
}

export interface ActionEntity {
    id: number;
    timestamp: string;
    sourceId: string;
    canRead: boolean;
    action?: Action;
}

export interface Action {
    id: number;
    userIdentity: string;
    timestamp: string;
    sourceId: string;
    sourceName: string;
    sourceType: string;
    componentDetails?: ExtensionDetails | RemoteProcessGroupDetails;
    operation: string;
    actionDetails?: ConfigureActionDetails | MoveActionDetails | ConnectionActionDetails | PurgeActionDetails;
}

export interface ExtensionDetails {
    type: string;
}

export interface RemoteProcessGroupDetails {
    uri: string;
}

export interface ConfigureActionDetails {
    name: string;
    previousValue: string;
    value: string;
}

export interface MoveActionDetails {
    previousGroupId: string;
    previousGroup: string;
    groupId: string;
    group: string;
}

export interface ConnectionActionDetails {
    sourceId: string;
    sourceName: string;
    sourceType: string;
    relationship: string;
    destinationId: string;
    destinationName: string;
    destinationType: string;
}

export interface PurgeActionDetails {
    endDate: string;
}

export interface HistoryQueryRequest {
    count: number;
    offset: number;
    sortColumn?: string;
    sortOrder?: 'asc' | 'desc';
    startDate?: string; // MM/dd/yyyy HH:mm:ss
    endDate?: string; // MM/dd/yyyy HH:mm:ss
    userIdentity?: string;
    sourceId?: string;
}

export interface FlowConfigurationHistoryListingState {
    actions: ActionEntity[];
    total: number;
    query: HistoryQueryRequest | null;
    loadedTimestamp: string;
    purging: boolean;
    selectedId: number | null;
    status: 'pending' | 'loading' | 'success';
}

export interface SelectFlowConfigurationHistoryRequest {
    id: number;
}

export interface PurgeHistoryRequest {
    endDate: string; // MM/dd/yyyy HH:mm:ss
}
