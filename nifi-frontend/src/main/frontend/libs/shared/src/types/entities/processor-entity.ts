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

import { Bundle, Permissions } from '../rest-api.types';
import { PositionableComponentDTO, PositionableComponentEntityBase } from './component-entity';
import { ProcessingPerformanceStatusDTO } from './processing-performance-status-dto';
import { PropertyDescriptorDTO, RelationshipDTO } from './property-descriptor-dto';

/**
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/entity/ProcessorEntity.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface ProcessorEntity extends PositionableComponentEntityBase<ProcessorDTO> {
    operatePermissions: Permissions;
    status?: ProcessorStatusDTO;
    inputRequirement: string;
    physicalState: string;
}

export interface ProcessorDTO extends PositionableComponentDTO {
    name: string;
    type: string;
    bundle: Bundle;
    state: string;
    relationships: RelationshipDTO[];
    supportsParallelProcessing: boolean;
    supportsBatching: boolean;
    supportsSensitiveDynamicProperties: boolean;
    supportsBacklogReporting: boolean;
    persistsState: boolean;
    restricted: boolean;
    deprecated: boolean;
    extensionMissing: boolean;
    executionNodeRestricted: boolean;
    multipleVersionsAvailable: boolean;
    inputRequirement: string;
    physicalState: string;
    config: ProcessorConfigDTO;
    validationStatus: string;
    style?: Record<string, string>;
    description?: string;
    validationErrors?: string[];
}

export interface ProcessorConfigDTO {
    schedulingPeriod: string;
    schedulingStrategy: string;
    executionNode: string;
    penaltyDuration: string;
    yieldDuration: string;
    bulletinLevel: string;
    runDurationMillis: number;
    concurrentlySchedulableTaskCount: number;
    autoTerminatedRelationships: string[];
    lossTolerant: boolean;
    retryCount: number;
    retriedRelationships: string[];
    backoffMechanism: string;
    maxBackoffPeriod: string;
    properties?: Record<string, string | null>;
    descriptors?: Record<string, PropertyDescriptorDTO>;
    annotationData?: string;
    defaultConcurrentTasks?: Record<string, string>;
    defaultSchedulingPeriod?: Record<string, string>;
    sensitiveDynamicPropertyNames?: string[];
    comments?: string;
    customUiUrl?: string;
}

export interface ProcessorStatusDTO {
    groupId: string;
    id: string;
    runStatus: string;
    statsLastRefreshed: string;
    aggregateSnapshot: ProcessorStatusSnapshotDTO;
    name?: string;
    type?: string;
    nodeSnapshots?: NodeProcessorStatusSnapshotDTO[];
}

export interface ProcessorStatusSnapshotDTO {
    id: string;
    groupId: string;
    runStatus: string;
    executionNode: string;
    bytesRead: number;
    bytesWritten: number;
    read: string;
    written: string;
    flowFilesIn: number;
    bytesIn: number;
    input: string;
    flowFilesOut: number;
    bytesOut: number;
    output: string;
    taskCount: number;
    tasksDurationNanos: number;
    tasks: string;
    tasksDuration: string;
    activeThreadCount: number;
    terminatedThreadCount: number;
    name?: string;
    type?: string;
    processingPerformanceStatus?: ProcessingPerformanceStatusDTO;
}

export interface NodeProcessorStatusSnapshotDTO {
    nodeId: string;
    address: string;
    apiPort: number;
    statusSnapshot: ProcessorStatusSnapshotDTO;
}

export interface ProcessorStatusSnapshotEntity {
    id: string;
    canRead: boolean;
    processorStatusSnapshot: ProcessorStatusSnapshotDTO;
}
