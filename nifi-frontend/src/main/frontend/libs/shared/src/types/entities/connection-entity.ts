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

import { Position } from '../rest-api.types';
import { ComponentDTO, ComponentEntityBase } from './component-entity';

/**
 * Connections intentionally have no entity-level position. Their geometry is
 * represented by bends and labelIndex.
 *
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/entity/ConnectionEntity.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface ConnectionEntity extends ComponentEntityBase<ConnectionDTO> {
    zIndex: number;
    bends: Position[];
    labelIndex: number;
    sourceId: string;
    sourceGroupId: string;
    sourceType: ConnectableComponentType;
    destinationId: string;
    destinationGroupId: string;
    destinationType: ConnectableComponentType;
    status?: ConnectionStatusDTO;
}

export type ConnectableComponentType =
    | 'PROCESSOR'
    | 'REMOTE_INPUT_PORT'
    | 'REMOTE_OUTPUT_PORT'
    | 'INPUT_PORT'
    | 'OUTPUT_PORT'
    | 'FUNNEL'
    | 'STATELESS_GROUP';

export interface ConnectionDTO extends ComponentDTO {
    source: ConnectableDTO;
    destination: ConnectableDTO;
    labelIndex: number;
    zIndex: number;
    backPressureObjectThreshold: number;
    backPressureDataSizeThreshold: string;
    flowFileExpiration: string;
    prioritizers: string[];
    bends: Position[];
    loadBalanceStrategy: string;
    loadBalanceCompression: string;
    loadBalanceStatus: string;
    name?: string;
    selectedRelationships?: string[];
    availableRelationships?: string[];
    retriedRelationships?: string[];
    loadBalancePartitionAttribute?: string;
}

export interface ConnectableDTO {
    id: string;
    type: ConnectableComponentType;
    groupId: string;
    name: string;
    running: boolean;
    versionedComponentId?: string;
    transmitting?: boolean;
    exists?: boolean;
    comments?: string;
}

export interface ConnectionStatusDTO {
    id: string;
    groupId: string;
    sourceId: string;
    destinationId: string;
    statsLastRefreshed: string;
    aggregateSnapshot: ConnectionStatusSnapshotDTO;
    name: string;
    sourceName: string;
    destinationName: string;
    nodeSnapshots?: NodeConnectionStatusSnapshotDTO[];
}

export interface ConnectionStatusSnapshotDTO {
    id: string;
    groupId: string;
    flowFilesIn: number;
    bytesIn: number;
    input: string;
    flowFilesOut: number;
    bytesOut: number;
    output: string;
    flowFilesQueued: number;
    bytesQueued: number;
    queued: string;
    queuedSize: string;
    queuedCount: string;
    flowFileAvailability: string;
    loadBalanceStatus: string;
    name: string;
    sourceId?: string;
    sourceName: string;
    destinationId?: string;
    destinationName: string;
    predictions?: ConnectionStatusPredictionsSnapshotDTO;
    percentUseCount?: number;
    percentUseBytes?: number;
}

export interface NodeConnectionStatusSnapshotDTO {
    nodeId: string;
    address: string;
    apiPort: number;
    statusSnapshot: ConnectionStatusSnapshotDTO;
}

export interface ConnectionStatusSnapshotEntity {
    id: string;
    canRead: boolean;
    connectionStatusSnapshot: ConnectionStatusSnapshotDTO;
}

export interface ConnectionStatusPredictionsSnapshotDTO {
    predictedMillisUntilCountBackpressure?: number;
    predictedMillisUntilBytesBackpressure?: number;
    predictionIntervalSeconds?: number;
    predictedCountAtNextInterval?: number;
    predictedBytesAtNextInterval?: number;
    predictedPercentCount?: number;
    predictedPercentBytes?: number;
}
