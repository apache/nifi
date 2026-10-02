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

import {
    ConnectableDTO,
    ConnectionDTO,
    ConnectionEntity,
    FunnelEntity,
    LabelDTO,
    LabelEntity,
    Permissions,
    PortDTO,
    PortEntity,
    ProcessGroupDTO,
    ProcessGroupEntity,
    ProcessorDTO,
    ProcessorEntity,
    RemoteProcessGroupDTO,
    RemoteProcessGroupEntity,
    Revision
} from '@nifi/shared';

function defaultPermissions(): Permissions {
    return { canRead: true, canWrite: true };
}

function defaultRevision(): Revision {
    return { version: 1 };
}

export function makeProcessor(id: string, overrides: Partial<ProcessorEntity> = {}): ProcessorEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/processors/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        operatePermissions: defaultPermissions(),
        position: { x: 0, y: 0 },
        inputRequirement: 'INPUT_ALLOWED',
        physicalState: 'STOPPED',
        ...overrides
    };
}

export function makeConnection(id: string, overrides: Partial<ConnectionEntity> = {}): ConnectionEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/connections/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        sourceId: `${id}-source`,
        sourceGroupId: 'pg-current',
        sourceType: 'PROCESSOR',
        destinationId: `${id}-destination`,
        destinationGroupId: 'pg-current',
        destinationType: 'PROCESSOR',
        bends: [],
        labelIndex: 0,
        zIndex: 0,
        ...overrides
    };
}

export function makePort(id: string, overrides: Partial<PortEntity> = {}): PortEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/input-ports/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        operatePermissions: defaultPermissions(),
        position: { x: 0, y: 0 },
        portType: 'INPUT_PORT',
        ...overrides
    };
}

export function makeProcessGroup(id: string, overrides: Partial<ProcessGroupEntity> = {}): ProcessGroupEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/process-groups/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        position: { x: 0, y: 0 },
        runningCount: 0,
        stoppedCount: 0,
        invalidCount: 0,
        disabledCount: 0,
        activeRemotePortCount: 0,
        inactiveRemotePortCount: 0,
        upToDateCount: 0,
        locallyModifiedCount: 0,
        staleCount: 0,
        locallyModifiedAndStaleCount: 0,
        syncFailureCount: 0,
        localInputPortCount: 0,
        localOutputPortCount: 0,
        publicInputPortCount: 0,
        publicOutputPortCount: 0,
        inputPortCount: 0,
        outputPortCount: 0,
        resolvedExecutionEngine: 'STANDARD',
        ...overrides
    };
}

export function makeRemoteProcessGroup(
    id: string,
    overrides: Partial<RemoteProcessGroupEntity> = {}
): RemoteProcessGroupEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/remote-process-groups/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        operatePermissions: defaultPermissions(),
        position: { x: 0, y: 0 },
        inputPortCount: 0,
        outputPortCount: 0,
        ...overrides
    };
}

export function makeFunnel(id: string, overrides: Partial<FunnelEntity> = {}): FunnelEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/funnels/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        position: { x: 0, y: 0 },
        ...overrides
    };
}

export function makeLabel(id: string, overrides: Partial<LabelEntity> = {}): LabelEntity {
    return {
        id,
        uri: `https://localhost/nifi-api/labels/${id}`,
        revision: defaultRevision(),
        permissions: defaultPermissions(),
        position: { x: 0, y: 0 },
        dimensions: { width: 200, height: 80 },
        zIndex: 0,
        ...overrides
    };
}

export function makeLabelDto(overrides: Partial<LabelDTO> = {}): LabelDTO {
    return {
        id: 'label-1',
        position: { x: 0, y: 0 },
        style: {},
        width: 200,
        height: 80,
        zIndex: 0,
        ...overrides
    };
}

export function makeConnectableDto(overrides: Partial<ConnectableDTO> = {}): ConnectableDTO {
    return {
        id: 'connectable-1',
        type: 'PROCESSOR',
        groupId: 'pg-current',
        name: 'connectable-1',
        running: false,
        ...overrides
    };
}

export function makeProcessorDto(overrides: Partial<ProcessorDTO> = {}): ProcessorDTO {
    return {
        id: 'proc-1',
        position: { x: 0, y: 0 },
        name: 'Processor',
        type: 'org.apache.nifi.processors.test.TestProcessor',
        bundle: { group: 'org.apache.nifi', artifact: 'nifi-standard-nar', version: '2.0.0' },
        state: 'STOPPED',
        relationships: [],
        supportsParallelProcessing: true,
        supportsBatching: false,
        supportsSensitiveDynamicProperties: false,
        supportsBacklogReporting: false,
        persistsState: false,
        restricted: false,
        deprecated: false,
        extensionMissing: false,
        executionNodeRestricted: false,
        multipleVersionsAvailable: false,
        inputRequirement: 'INPUT_ALLOWED',
        physicalState: 'STOPPED',
        config: {
            schedulingPeriod: '0 sec',
            schedulingStrategy: 'TIMER_DRIVEN',
            executionNode: 'ALL',
            penaltyDuration: '30 sec',
            yieldDuration: '1 sec',
            bulletinLevel: 'WARN',
            runDurationMillis: 0,
            concurrentlySchedulableTaskCount: 1,
            autoTerminatedRelationships: [],
            lossTolerant: false,
            retryCount: 10,
            retriedRelationships: [],
            backoffMechanism: 'PENALIZE_FLOWFILE',
            maxBackoffPeriod: '10 mins'
        },
        validationStatus: 'VALID',
        ...overrides
    };
}

export function makeConnectionDto(overrides: Partial<ConnectionDTO> = {}): ConnectionDTO {
    return {
        id: 'conn-1',
        source: makeConnectableDto(),
        destination: makeConnectableDto(),
        labelIndex: 0,
        zIndex: 0,
        backPressureObjectThreshold: 10000,
        backPressureDataSizeThreshold: '1 GB',
        flowFileExpiration: '0 sec',
        prioritizers: [],
        bends: [],
        loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE',
        loadBalanceCompression: 'DO_NOT_COMPRESS',
        loadBalanceStatus: 'LOAD_BALANCE_NOT_CONFIGURED',
        ...overrides
    };
}

export function makePortDto(overrides: Partial<PortDTO> = {}): PortDTO {
    return {
        id: 'port-1',
        position: { x: 0, y: 0 },
        name: 'Port',
        state: 'STOPPED',
        type: 'INPUT_PORT',
        portFunction: 'STANDARD',
        concurrentlySchedulableTaskCount: 1,
        ...overrides
    };
}

export function makeProcessGroupDto(overrides: Partial<ProcessGroupDTO> = {}): ProcessGroupDTO {
    return {
        id: 'pg-1',
        position: { x: 0, y: 0 },
        name: 'Process Group',
        flowfileConcurrency: 'UNBOUNDED',
        flowfileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
        defaultFlowFileExpiration: '0 sec',
        defaultBackPressureObjectThreshold: 10000,
        defaultBackPressureDataSizeThreshold: '1 GB',
        executionEngine: 'STANDARD',
        resolvedExecutionEngine: 'STANDARD',
        maxConcurrentTasks: 1,
        statelessFlowTimeout: '1 min',
        statelessGroupScheduledState: 'STOPPED',
        runningCount: 0,
        stoppedCount: 0,
        invalidCount: 0,
        disabledCount: 0,
        activeRemotePortCount: 0,
        inactiveRemotePortCount: 0,
        upToDateCount: 0,
        locallyModifiedCount: 0,
        staleCount: 0,
        locallyModifiedAndStaleCount: 0,
        syncFailureCount: 0,
        localInputPortCount: 0,
        localOutputPortCount: 0,
        publicInputPortCount: 0,
        publicOutputPortCount: 0,
        inputPortCount: 0,
        outputPortCount: 0,
        ...overrides
    };
}

export function makeRemoteProcessGroupDto(overrides: Partial<RemoteProcessGroupDTO> = {}): RemoteProcessGroupDTO {
    return {
        id: 'rpg-1',
        position: { x: 0, y: 0 },
        targetUris: 'http://localhost:8080/nifi',
        communicationsTimeout: '30 sec',
        yieldDuration: '10 sec',
        transportProtocol: 'RAW',
        transmitting: false,
        activeRemoteInputPortCount: 0,
        inactiveRemoteInputPortCount: 0,
        activeRemoteOutputPortCount: 0,
        inactiveRemoteOutputPortCount: 0,
        flowRefreshed: '2026-01-01T00:00:00.000Z',
        contents: { inputPorts: [], outputPorts: [] },
        ...overrides
    };
}
