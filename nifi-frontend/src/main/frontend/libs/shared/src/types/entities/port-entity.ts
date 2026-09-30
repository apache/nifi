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

export type PortType = 'INPUT_PORT' | 'OUTPUT_PORT';
export type PortFunction = 'STANDARD' | 'FAILURE';

/**
 * Shared input/output port response, discriminated by portType.
 *
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/entity/PortEntity.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface PortEntity extends PositionableComponentEntityBase<PortDTO> {
    operatePermissions: Permissions;
    status?: PortStatusDTO;
    portType: PortType;
    allowRemoteAccess?: boolean;
}

export interface PortDTO extends PositionableComponentDTO {
    state: string;
    type: PortType;
    portFunction: PortFunction;
    concurrentlySchedulableTaskCount: number;
    name: string;
    comments?: string;
    transmitting?: boolean;
    allowRemoteAccess?: boolean;
    validationErrors?: string[];
}

export interface PortStatusDTO {
    id: string;
    groupId: string;
    transmitting?: boolean;
    runStatus: string;
    statsLastRefreshed: string;
    aggregateSnapshot: PortStatusSnapshotDTO;
    name: string;
    nodeSnapshots?: NodePortStatusSnapshotDTO[];
}

export interface PortStatusSnapshotDTO {
    id: string;
    groupId: string;
    activeThreadCount: number;
    flowFilesIn: number;
    bytesIn: number;
    input: string;
    flowFilesOut: number;
    bytesOut: number;
    output: string;
    runStatus: string;
    transmitting?: boolean;
    name: string;
}

export interface NodePortStatusSnapshotDTO {
    nodeId: string;
    address: string;
    apiPort: number;
    statusSnapshot: PortStatusSnapshotDTO;
}

export interface PortStatusSnapshotEntity {
    id: string;
    canRead: boolean;
    portStatusSnapshot: PortStatusSnapshotDTO;
}
