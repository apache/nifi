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

import { Permissions } from '../rest-api.types';
import { PositionableComponentDTO, PositionableComponentEntityBase } from './component-entity';
import { ConnectionDTO, ConnectionStatusSnapshotEntity } from './connection-entity';
import { ControllerServiceDTO } from './controller-service-entity';
import { FunnelDTO } from './funnel-entity';
import { LabelDTO } from './label-entity';
import { PortDTO, PortStatusSnapshotEntity } from './port-entity';
import { ProcessingPerformanceStatusDTO } from './processing-performance-status-dto';
import { ProcessorDTO, ProcessorStatusSnapshotEntity } from './processor-entity';
import { RegisteredFlowSnapshot } from './registered-flow-snapshot';
import { RemoteProcessGroupDTO, RemoteProcessGroupStatusSnapshotEntity } from './remote-process-group-entity';

export type ResolvedExecutionEngine = 'STANDARD' | 'STATELESS';

/**
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/entity/ProcessGroupEntity.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface ProcessGroupEntity extends PositionableComponentEntityBase<ProcessGroupDTO> {
    status?: ProcessGroupStatusDTO;
    runningCount: number;
    stoppedCount: number;
    invalidCount: number;
    disabledCount: number;
    activeRemotePortCount: number;
    inactiveRemotePortCount: number;
    upToDateCount: number;
    locallyModifiedCount: number;
    staleCount: number;
    locallyModifiedAndStaleCount: number;
    syncFailureCount: number;
    localInputPortCount: number;
    localOutputPortCount: number;
    publicInputPortCount: number;
    publicOutputPortCount: number;
    inputPortCount: number;
    outputPortCount: number;
    resolvedExecutionEngine: ResolvedExecutionEngine;
    versionedFlowState?: string;
    parameterContext?: ParameterContextReferenceEntity;
    versionedFlowSnapshot?: RegisteredFlowSnapshot;
    processGroupUpdateStrategy?: 'DIRECT_CHILDREN' | 'ALL_DESCENDANTS';
}

export interface ProcessGroupDTO extends PositionableComponentDTO {
    name: string;
    flowfileConcurrency: string;
    flowfileOutboundPolicy: string;
    defaultFlowFileExpiration: string;
    defaultBackPressureObjectThreshold: number;
    defaultBackPressureDataSizeThreshold: string;
    executionEngine: string;
    resolvedExecutionEngine: ResolvedExecutionEngine;
    maxConcurrentTasks: number;
    statelessFlowTimeout: string;
    statelessGroupScheduledState: string;
    runningCount: number;
    stoppedCount: number;
    invalidCount: number;
    disabledCount: number;
    activeRemotePortCount: number;
    inactiveRemotePortCount: number;
    upToDateCount: number;
    locallyModifiedCount: number;
    staleCount: number;
    locallyModifiedAndStaleCount: number;
    syncFailureCount: number;
    localInputPortCount: number;
    localOutputPortCount: number;
    publicInputPortCount: number;
    publicOutputPortCount: number;
    inputPortCount: number;
    outputPortCount: number;
    comments?: string;
    logFileSuffix?: string;
    versionControlInformation?: VersionControlInformationDTO;
    parameterContext?: ParameterContextReferenceEntity;
    contents?: FlowSnippetDTO;
}

export interface ParameterContextReferenceEntity {
    id: string;
    permissions: Permissions;
    component?: ParameterContextReferenceDTO;
}

export interface ParameterContextReferenceDTO {
    id: string;
    name: string;
}

export interface FlowSnippetDTO {
    processGroups: ProcessGroupDTO[];
    remoteProcessGroups: RemoteProcessGroupDTO[];
    processors: ProcessorDTO[];
    inputPorts: PortDTO[];
    outputPorts: PortDTO[];
    connections: ConnectionDTO[];
    labels: LabelDTO[];
    funnels: FunnelDTO[];
    controllerServices: ControllerServiceDTO[];
}

export interface VersionControlInformationDTO {
    groupId: string;
    registryId: string;
    registryName?: string;
    branch?: string;
    bucketId: string;
    bucketName?: string;
    flowId: string;
    flowName: string;
    flowDescription?: string;
    version: string;
    storageLocation?: string;
    state?: string;
    stateExplanation?: string;
}

export interface ProcessGroupStatusDTO {
    id: string;
    statsLastRefreshed: string;
    aggregateSnapshot: ProcessGroupStatusSnapshotDTO;
    name: string;
    nodeSnapshots?: NodeProcessGroupStatusSnapshotDTO[];
}

export interface ProcessGroupStatusSnapshotDTO {
    id: string;
    statelessActiveThreadCount: number;
    flowFilesIn: number;
    bytesIn: number;
    input: string;
    flowFilesQueued: number;
    bytesQueued: number;
    queued: string;
    queuedCount: string;
    queuedSize: string;
    bytesRead: number;
    read: string;
    bytesWritten: number;
    written: string;
    flowFilesOut: number;
    bytesOut: number;
    output: string;
    flowFilesTransferred: number;
    bytesTransferred: number;
    transferred: string;
    bytesReceived: number;
    flowFilesReceived: number;
    received: string;
    bytesSent: number;
    flowFilesSent: number;
    sent: string;
    activeThreadCount: number;
    terminatedThreadCount: number;
    processingNanos: number;
    name: string;
    connectionStatusSnapshots?: ConnectionStatusSnapshotEntity[];
    processorStatusSnapshots?: ProcessorStatusSnapshotEntity[];
    processGroupStatusSnapshots?: ProcessGroupStatusSnapshotEntity[];
    remoteProcessGroupStatusSnapshots?: RemoteProcessGroupStatusSnapshotEntity[];
    inputPortStatusSnapshots?: PortStatusSnapshotEntity[];
    outputPortStatusSnapshots?: PortStatusSnapshotEntity[];
    versionedFlowState?: string;
    processingPerformanceStatus?: ProcessingPerformanceStatusDTO;
}

export interface NodeProcessGroupStatusSnapshotDTO {
    nodeId: string;
    address: string;
    apiPort: number;
    statusSnapshot: ProcessGroupStatusSnapshotDTO;
}

export interface ProcessGroupStatusSnapshotEntity {
    id: string;
    canRead: boolean;
    processGroupStatusSnapshot: ProcessGroupStatusSnapshotDTO;
}
