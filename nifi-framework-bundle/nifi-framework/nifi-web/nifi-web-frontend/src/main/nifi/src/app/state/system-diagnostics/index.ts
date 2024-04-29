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
export const systemDiagnosticsFeatureKey = 'systemDiagnostics';

export interface SystemDiagnostics {
    aggregateSnapshot: SystemDiagnosticSnapshot;
    nodeSnapshots?: NodeSnapshot[];
}

export interface RepositoryStorageUsage {
    freeSpace: string;
    freeSpaceBytes: number;
    totalSpace: string;
    totalSpaceBytes: number;
    usedSpace: string;
    usedSpaceBytes: number;
    utilization: string;
    identifier?: string;
}
export interface GarbageCollection {
    collectionCount: number;
    collectionMillis: number;
    collectionTime: string;
    name: string;
}

export interface VersionInfo {
    buildBranch: string;
    buildRevision: string;
    buildTag: string;
    buildTimestamp: string;
    javaVendor: string;
    javaVersion: string;
    niFiVersion: string;
    osArchitecture: string;
    osName: string;
    osVersion: string;
}

export interface NodeSnapshot {
    address: string;
    apiPort: number;
    nodeId: string;
    snapshot: SystemDiagnosticSnapshot;
}

export interface SystemDiagnosticSnapshot {
    availableProcessors: number;
    contentRepositoryStorageUsage: RepositoryStorageUsage[];
    daemonThreads: number;
    flowFileRepositoryStorageUsage: RepositoryStorageUsage;
    freeHeap: string;
    freeHeapBytes: number;
    freeNonHeap: string;
    freeNonHeapBytes: number;
    garbageCollection: GarbageCollection[];
    heapUtilization: string;
    maxHeap: string;
    maxHeapBytes: number;
    maxNonHeap: string;
    maxNonHeapBytes: number;
    processorLoadAverage: number;
    provenanceRepositoryStorageUsage: RepositoryStorageUsage[];
    statsLastRefreshed: string;
    totalHeap: string;
    totalHeapBytes: number;
    totalNonHeap: string;
    totalNonHeapBytes: number;
    totalThreads: number;
    uptime: string;
    usedHeap: string;
    usedHeapBytes: number;
    usedNonHeap: string;
    usedNonHeapBytes: number;
    versionInfo: VersionInfo;
}

export interface SystemDiagnosticsRequest {
    nodewise: boolean;
    errorStrategy?: 'banner' | 'snackbar';
}

export interface SystemDiagnosticsResponse {
    systemDiagnostics: SystemDiagnostics;
}

export interface SystemDiagnosticsState {
    systemDiagnostics: SystemDiagnostics | null;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'error' | 'success';
}

export interface ClusterNodeRepositoryStorageUsage {
    address: string;
    apiPort: number;
    nodeId: string;
    repositoryStorageUsage: RepositoryStorageUsage;
}
