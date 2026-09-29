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

/**
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/entity/RemoteProcessGroupEntity.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface RemoteProcessGroupEntity extends PositionableComponentEntityBase<RemoteProcessGroupDTO> {
    operatePermissions: Permissions;
    status?: RemoteProcessGroupStatusDTO;
    inputPortCount?: number;
    outputPortCount?: number;
}

export interface RemoteProcessGroupDTO extends PositionableComponentDTO {
    targetUris: string;
    communicationsTimeout: string;
    yieldDuration: string;
    transportProtocol: string;
    transmitting: boolean;
    activeRemoteInputPortCount: number;
    inactiveRemoteInputPortCount: number;
    activeRemoteOutputPortCount: number;
    inactiveRemoteOutputPortCount: number;
    flowRefreshed: string;
    contents: RemoteProcessGroupContentsDTO;
    targetUri?: string;
    name?: string;
    comments?: string;
    targetSecure?: boolean;
    localNetworkInterface?: string;
    proxyHost?: string;
    proxyPort?: number;
    proxyUser?: string;
    proxyPassword?: string;
    authorizationIssues?: string[];
    validationErrors?: string[];
    inputPortCount?: number;
    outputPortCount?: number;
}

export interface RemoteProcessGroupContentsDTO {
    inputPorts: RemoteProcessGroupPortDTO[];
    outputPorts: RemoteProcessGroupPortDTO[];
}

export interface RemoteProcessGroupPortDTO {
    id: string;
    groupId: string;
    concurrentlySchedulableTaskCount: number;
    transmitting: boolean;
    useCompression: boolean;
    exists: boolean;
    targetRunning: boolean;
    connected: boolean;
    batchSettings: BatchSettingsDTO;
    targetId?: string;
    versionedComponentId?: string;
    name?: string;
    comments?: string;
}

export interface BatchSettingsDTO {
    count: number;
    size?: string;
    duration?: string;
}

export interface RemoteProcessGroupStatusDTO {
    groupId: string;
    id: string;
    transmissionStatus: string;
    statsLastRefreshed: string;
    aggregateSnapshot: RemoteProcessGroupStatusSnapshotDTO;
    name?: string;
    targetUri?: string;
    validationStatus: string;
    nodeSnapshots?: NodeRemoteProcessGroupStatusSnapshotDTO[];
}

export interface RemoteProcessGroupStatusSnapshotDTO {
    id: string;
    groupId: string;
    transmissionStatus: string;
    activeThreadCount: number;
    flowFilesSent: number;
    bytesSent: number;
    sent: string;
    flowFilesReceived: number;
    bytesReceived: number;
    received: string;
    name?: string;
    targetUri?: string;
}

export interface NodeRemoteProcessGroupStatusSnapshotDTO {
    nodeId: string;
    address: string;
    apiPort: number;
    statusSnapshot: RemoteProcessGroupStatusSnapshotDTO;
}

export interface RemoteProcessGroupStatusSnapshotEntity {
    id: string;
    canRead: boolean;
    remoteProcessGroupStatusSnapshot: RemoteProcessGroupStatusSnapshotDTO;
}
