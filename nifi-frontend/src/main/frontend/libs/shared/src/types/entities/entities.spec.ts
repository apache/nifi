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

import { describe, expect, expectTypeOf, it } from 'vitest';
import { BulletinEntity } from '../rest-api.types';
import { ComponentEntityBase, PositionableComponentDTO } from './component-entity';
import {
    ConnectableDTO,
    ConnectionDTO,
    ConnectionEntity,
    ConnectionStatusDTO,
    ConnectionStatusSnapshotDTO,
    ConnectionStatusSnapshotEntity
} from './connection-entity';
import { ControllerServiceReferencingComponentEntity } from './controller-service-entity';
import { FunnelDTO, FunnelEntity } from './funnel-entity';
import { LabelDTO, LabelEntity } from './label-entity';
import { PortDTO, PortEntity, PortStatusDTO, PortStatusSnapshotDTO, PortStatusSnapshotEntity } from './port-entity';
import {
    FlowSnippetDTO,
    ProcessGroupDTO,
    ProcessGroupEntity,
    ProcessGroupStatusDTO,
    ProcessGroupStatusSnapshotDTO,
    ProcessGroupStatusSnapshotEntity
} from './process-group-entity';
import {
    ProcessorConfigDTO,
    ProcessorDTO,
    ProcessorEntity,
    ProcessorStatusDTO,
    ProcessorStatusSnapshotDTO,
    ProcessorStatusSnapshotEntity
} from './processor-entity';
import { AllowableValueDTO, AllowableValueEntity } from './property-descriptor-dto';
import { VersionedProcessGroup } from './registered-flow-snapshot';
import {
    RemoteProcessGroupDTO,
    RemoteProcessGroupEntity,
    RemoteProcessGroupStatusDTO,
    RemoteProcessGroupStatusSnapshotDTO,
    RemoteProcessGroupStatusSnapshotEntity
} from './remote-process-group-entity';

const permissions = { canRead: true, canWrite: true };
const deniedPermissions = { canRead: false, canWrite: false };
const position = { x: 10, y: 20 };

function entityEnvelope(id: string): ComponentEntityBase<never> {
    return {
        id,
        uri: `/components/${id}`,
        revision: { version: 1, clientId: 'client-id', lastModifier: 'user' },
        permissions
    };
}

function component(id: string): PositionableComponentDTO {
    return { id, parentGroupId: 'root', position };
}

function processorConfig(): ProcessorConfigDTO {
    return {
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
        maxBackoffPeriod: '10 mins',
        properties: { destination: '/tmp' },
        descriptors: {
            destination: {
                name: 'destination',
                displayName: 'Destination',
                required: true,
                sensitive: false,
                dynamic: false,
                supportsEl: true,
                dependencies: []
            }
        }
    };
}

function processor(id: string): ProcessorDTO {
    return {
        ...component(id),
        name: 'Processor',
        type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
        bundle: { group: 'org.apache.nifi', artifact: 'nifi-standard-nar', version: '2.0.0' },
        state: 'STOPPED',
        relationships: [{ name: 'success', autoTerminate: false, retry: false }],
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
        inputRequirement: 'INPUT_FORBIDDEN',
        physicalState: 'STOPPED',
        config: processorConfig(),
        validationStatus: 'VALID'
    };
}

function processorSnapshot(id: string): ProcessorStatusSnapshotDTO {
    return {
        id,
        groupId: 'root',
        name: 'Processor',
        type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
        runStatus: 'Stopped',
        executionNode: 'All',
        bytesRead: 1,
        bytesWritten: 2,
        read: '1 byte',
        written: '2 bytes',
        flowFilesIn: 3,
        bytesIn: 4,
        input: '3 / 4 bytes',
        flowFilesOut: 5,
        bytesOut: 6,
        output: '5 / 6 bytes',
        taskCount: 7,
        tasksDurationNanos: 8,
        tasks: '7',
        tasksDuration: '8 ns',
        activeThreadCount: 0,
        terminatedThreadCount: 0,
        processingPerformanceStatus: {
            identifier: id,
            cpuDuration: 1,
            contentReadDuration: 2,
            contentWriteDuration: 3,
            sessionCommitDuration: 4,
            garbageCollectionDuration: 5
        }
    };
}

function processorStatus(id: string): ProcessorStatusDTO {
    return {
        id,
        groupId: 'root',
        name: 'Processor',
        runStatus: 'Stopped',
        statsLastRefreshed: '12:00:00 UTC',
        aggregateSnapshot: processorSnapshot(id)
    };
}

function connectable(id: string, type: ConnectableDTO['type']): ConnectableDTO {
    return { id, groupId: 'root', type, name: id, running: false };
}

function connection(id: string): ConnectionDTO {
    return {
        ...component(id),
        source: connectable('source', 'PROCESSOR'),
        destination: connectable('destination', 'INPUT_PORT'),
        labelIndex: 0,
        zIndex: 1,
        backPressureObjectThreshold: 10000,
        backPressureDataSizeThreshold: '1 GB',
        flowFileExpiration: '0 sec',
        prioritizers: [],
        bends: [position],
        loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE',
        loadBalanceCompression: 'DO_NOT_COMPRESS',
        loadBalanceStatus: 'LOAD_BALANCE_NOT_CONFIGURED'
    };
}

function connectionSnapshot(id: string): ConnectionStatusSnapshotDTO {
    return {
        id,
        groupId: 'root',
        name: 'Connection',
        sourceName: 'Source',
        destinationName: 'Destination',
        flowFilesIn: 1,
        bytesIn: 2,
        input: '1 / 2 bytes',
        flowFilesOut: 3,
        bytesOut: 4,
        output: '3 / 4 bytes',
        flowFilesQueued: 5,
        bytesQueued: 6,
        queued: '5 / 6 bytes',
        queuedSize: '6 bytes',
        queuedCount: '5',
        flowFileAvailability: 'AVAILABLE',
        loadBalanceStatus: 'LOAD_BALANCE_NOT_CONFIGURED'
    };
}

function connectionStatus(id: string): ConnectionStatusDTO {
    return {
        id,
        groupId: 'root',
        name: 'Connection',
        sourceId: 'source',
        sourceName: 'Source',
        destinationId: 'destination',
        destinationName: 'Destination',
        statsLastRefreshed: '12:00:00 UTC',
        aggregateSnapshot: connectionSnapshot(id)
    };
}

function port(id: string, type: PortDTO['type']): PortDTO {
    return {
        ...component(id),
        name: id,
        state: 'STOPPED',
        type,
        portFunction: 'STANDARD',
        concurrentlySchedulableTaskCount: 1,
        allowRemoteAccess: false
    };
}

function portSnapshot(id: string): PortStatusSnapshotDTO {
    return {
        id,
        groupId: 'root',
        name: 'Port',
        activeThreadCount: 0,
        flowFilesIn: 1,
        bytesIn: 2,
        input: '1 / 2 bytes',
        flowFilesOut: 3,
        bytesOut: 4,
        output: '3 / 4 bytes',
        runStatus: 'Stopped'
    };
}

function portStatus(id: string): PortStatusDTO {
    return {
        id,
        groupId: 'root',
        name: 'Port',
        transmitting: false,
        runStatus: 'Stopped',
        statsLastRefreshed: '12:00:00 UTC',
        aggregateSnapshot: portSnapshot(id)
    };
}

function emptyContents(): FlowSnippetDTO {
    return {
        processGroups: [],
        remoteProcessGroups: [],
        processors: [],
        inputPorts: [],
        outputPorts: [],
        connections: [],
        labels: [],
        funnels: [],
        controllerServices: []
    };
}

function processGroup(id: string): ProcessGroupDTO {
    return {
        ...component(id),
        name: 'Process Group',
        flowfileConcurrency: 'UNBOUNDED',
        flowfileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
        defaultFlowFileExpiration: '0 sec',
        defaultBackPressureObjectThreshold: 10000,
        defaultBackPressureDataSizeThreshold: '1 GB',
        executionEngine: 'INHERITED',
        resolvedExecutionEngine: 'STANDARD',
        maxConcurrentTasks: 1,
        statelessFlowTimeout: '1 min',
        statelessGroupScheduledState: 'STOPPED',
        runningCount: 0,
        stoppedCount: 1,
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
        contents: emptyContents()
    };
}

function processGroupSnapshot(id: string): ProcessGroupStatusSnapshotDTO {
    return {
        id,
        name: 'Process Group',
        statelessActiveThreadCount: 0,
        flowFilesIn: 1,
        bytesIn: 2,
        input: '1 / 2 bytes',
        flowFilesQueued: 3,
        bytesQueued: 4,
        queued: '3 / 4 bytes',
        queuedCount: '3',
        queuedSize: '4 bytes',
        bytesRead: 5,
        read: '5 bytes',
        bytesWritten: 6,
        written: '6 bytes',
        flowFilesOut: 7,
        bytesOut: 8,
        output: '7 / 8 bytes',
        flowFilesTransferred: 9,
        bytesTransferred: 10,
        transferred: '9 / 10 bytes',
        bytesReceived: 11,
        flowFilesReceived: 12,
        received: '12 / 11 bytes',
        bytesSent: 13,
        flowFilesSent: 14,
        sent: '14 / 13 bytes',
        activeThreadCount: 0,
        terminatedThreadCount: 0,
        processingNanos: 15,
        processorStatusSnapshots: [
            {
                id: 'processor',
                canRead: true,
                processorStatusSnapshot: processorSnapshot('processor')
            }
        ]
    };
}

function processGroupStatus(id: string): ProcessGroupStatusDTO {
    return {
        id,
        name: 'Process Group',
        statsLastRefreshed: '12:00:00 UTC',
        aggregateSnapshot: processGroupSnapshot(id)
    };
}

function remoteProcessGroup(id: string): RemoteProcessGroupDTO {
    return {
        ...component(id),
        targetUris: 'https://example.test/nifi',
        communicationsTimeout: '30 sec',
        yieldDuration: '10 sec',
        transportProtocol: 'RAW',
        transmitting: false,
        activeRemoteInputPortCount: 0,
        inactiveRemoteInputPortCount: 0,
        activeRemoteOutputPortCount: 0,
        inactiveRemoteOutputPortCount: 0,
        flowRefreshed: '12:00:00 UTC',
        contents: {
            inputPorts: [
                {
                    id: 'remote-input',
                    groupId: id,
                    concurrentlySchedulableTaskCount: 1,
                    transmitting: false,
                    useCompression: false,
                    exists: true,
                    targetRunning: false,
                    connected: true,
                    batchSettings: { count: 1 }
                }
            ],
            outputPorts: []
        }
    };
}

function remoteProcessGroupSnapshot(id: string): RemoteProcessGroupStatusSnapshotDTO {
    return {
        id,
        groupId: 'root',
        name: 'Remote Process Group',
        transmissionStatus: 'Not Transmitting',
        activeThreadCount: 0,
        flowFilesSent: 1,
        bytesSent: 2,
        sent: '1 / 2 bytes',
        flowFilesReceived: 3,
        bytesReceived: 4,
        received: '3 / 4 bytes'
    };
}

function remoteProcessGroupStatus(id: string): RemoteProcessGroupStatusDTO {
    return {
        id,
        groupId: 'root',
        name: 'Remote Process Group',
        transmissionStatus: 'Not Transmitting',
        statsLastRefreshed: '12:00:00 UTC',
        aggregateSnapshot: remoteProcessGroupSnapshot(id),
        validationStatus: 'VALID'
    };
}

function createProcessorEntity(componentValue: ProcessorDTO, status: ProcessorStatusDTO): ProcessorEntity {
    return {
        ...entityEnvelope(componentValue.id),
        position,
        component: componentValue,
        status,
        operatePermissions: permissions,
        inputRequirement: componentValue.inputRequirement,
        physicalState: componentValue.physicalState
    };
}

function createConnectionEntity(componentValue: ConnectionDTO, status: ConnectionStatusDTO): ConnectionEntity {
    return {
        ...entityEnvelope(componentValue.id),
        component: componentValue,
        status,
        zIndex: componentValue.zIndex,
        bends: componentValue.bends,
        labelIndex: componentValue.labelIndex,
        sourceId: componentValue.source.id,
        sourceGroupId: componentValue.source.groupId,
        sourceType: componentValue.source.type,
        destinationId: componentValue.destination.id,
        destinationGroupId: componentValue.destination.groupId,
        destinationType: componentValue.destination.type
    };
}

function createPortEntity(componentValue: PortDTO, status: PortStatusDTO): PortEntity {
    return {
        ...entityEnvelope(componentValue.id),
        position,
        component: componentValue,
        status,
        operatePermissions: permissions,
        portType: componentValue.type,
        allowRemoteAccess: componentValue.allowRemoteAccess
    };
}

function createProcessGroupEntity(componentValue: ProcessGroupDTO, status: ProcessGroupStatusDTO): ProcessGroupEntity {
    return {
        ...entityEnvelope(componentValue.id),
        position,
        component: componentValue,
        status,
        runningCount: componentValue.runningCount,
        stoppedCount: componentValue.stoppedCount,
        invalidCount: componentValue.invalidCount,
        disabledCount: componentValue.disabledCount,
        activeRemotePortCount: componentValue.activeRemotePortCount,
        inactiveRemotePortCount: componentValue.inactiveRemotePortCount,
        upToDateCount: componentValue.upToDateCount,
        locallyModifiedCount: componentValue.locallyModifiedCount,
        staleCount: componentValue.staleCount,
        locallyModifiedAndStaleCount: componentValue.locallyModifiedAndStaleCount,
        syncFailureCount: componentValue.syncFailureCount,
        localInputPortCount: componentValue.localInputPortCount,
        localOutputPortCount: componentValue.localOutputPortCount,
        publicInputPortCount: componentValue.publicInputPortCount,
        publicOutputPortCount: componentValue.publicOutputPortCount,
        inputPortCount: componentValue.inputPortCount,
        outputPortCount: componentValue.outputPortCount,
        resolvedExecutionEngine: componentValue.resolvedExecutionEngine
    };
}

function createRemoteProcessGroupEntity(
    componentValue: RemoteProcessGroupDTO,
    status: RemoteProcessGroupStatusDTO
): RemoteProcessGroupEntity {
    return {
        ...entityEnvelope(componentValue.id),
        position,
        component: componentValue,
        status,
        operatePermissions: permissions,
        inputPortCount: componentValue.inputPortCount,
        outputPortCount: componentValue.outputPortCount
    };
}

function createFunnelEntity(componentValue: FunnelDTO): FunnelEntity {
    return { ...entityEnvelope(componentValue.id), position, component: componentValue };
}

function createLabelEntity(componentValue: LabelDTO): LabelEntity {
    return {
        ...entityEnvelope(componentValue.id),
        position,
        component: componentValue,
        dimensions: { width: componentValue.width, height: componentValue.height },
        zIndex: componentValue.zIndex
    };
}

describe('canonical response entity contracts', () => {
    it('represents readable forms for every canonical canvas entity kind', () => {
        const inputPort = createPortEntity(port('input', 'INPUT_PORT'), portStatus('input'));
        const outputPort = createPortEntity(port('output', 'OUTPUT_PORT'), portStatus('output'));
        const entities = [
            createProcessorEntity(processor('processor'), processorStatus('processor')),
            createConnectionEntity(connection('connection'), connectionStatus('connection')),
            inputPort,
            createProcessGroupEntity(processGroup('group'), processGroupStatus('group')),
            createRemoteProcessGroupEntity(
                remoteProcessGroup('remote-group'),
                remoteProcessGroupStatus('remote-group')
            ),
            createFunnelEntity(component('funnel')),
            createLabelEntity({
                ...component('label'),
                label: 'Label',
                style: {},
                width: 200,
                height: 80,
                zIndex: 2
            })
        ];

        expect(entities.map((entity) => entity.component?.id)).toEqual([
            'processor',
            'connection',
            'input',
            'group',
            'remote-group',
            'funnel',
            'label'
        ]);
        expectTypeOf(inputPort.component?.type).toEqualTypeOf<'INPUT_PORT' | 'OUTPUT_PORT' | undefined>();
        expectTypeOf(outputPort.portType).toEqualTypeOf<'INPUT_PORT' | 'OUTPUT_PORT'>();
        expectTypeOf(entities[0].revision.lastModifier).toEqualTypeOf<string | undefined>();
    });

    it('represents denied-read forms without component DTOs', () => {
        const deniedProcessor: ProcessorEntity = {
            ...entityEnvelope('processor'),
            permissions: deniedPermissions,
            position,
            operatePermissions: permissions,
            inputRequirement: 'INPUT_FORBIDDEN',
            physicalState: 'STOPPED'
        };
        const deniedConnection: ConnectionEntity = {
            ...entityEnvelope('connection'),
            permissions: deniedPermissions,
            zIndex: 1,
            bends: [],
            labelIndex: 0,
            sourceId: 'source',
            sourceGroupId: 'root',
            sourceType: 'PROCESSOR',
            destinationId: 'destination',
            destinationGroupId: 'root',
            destinationType: 'INPUT_PORT'
        };
        const deniedPort: PortEntity = {
            ...entityEnvelope('port'),
            permissions: deniedPermissions,
            position,
            operatePermissions: permissions,
            portType: 'INPUT_PORT'
        };
        const deniedProcessGroup: ProcessGroupEntity = {
            ...entityEnvelope('group'),
            permissions: deniedPermissions,
            position,
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
            resolvedExecutionEngine: 'STANDARD'
        };
        const deniedRemoteProcessGroup: RemoteProcessGroupEntity = {
            ...entityEnvelope('remote-group'),
            permissions: deniedPermissions,
            position,
            operatePermissions: permissions
        };
        const deniedFunnel: FunnelEntity = {
            ...entityEnvelope('funnel'),
            permissions: deniedPermissions,
            position
        };
        const deniedLabel: LabelEntity = {
            ...entityEnvelope('label'),
            permissions: deniedPermissions,
            position,
            dimensions: { width: 0, height: 0 },
            zIndex: 0
        };
        const deniedEntities = [
            deniedProcessor,
            deniedConnection,
            deniedPort,
            deniedProcessGroup,
            deniedRemoteProcessGroup,
            deniedFunnel,
            deniedLabel
        ];

        expect(deniedEntities.every((entity) => entity.component === undefined)).toBe(true);
        expect(deniedEntities.every((entity) => !entity.permissions.canRead)).toBe(true);
    });

    it('models permission-gated bulletins and embedded controller-service references', () => {
        const deniedBulletin: BulletinEntity = {
            canRead: false,
            id: 1,
            sourceId: 'processor',
            groupId: 'root',
            timestamp: '12:00:00 UTC',
            timestampIso: '2026-09-29T12:00:00Z'
        };
        const readableReference: ControllerServiceReferencingComponentEntity = {
            id: 'reference',
            revision: { version: 1 },
            permissions,
            operatePermissions: permissions,
            component: {
                id: 'reference',
                name: 'Processor',
                properties: {},
                descriptors: {}
            }
        };
        const deniedReference: ControllerServiceReferencingComponentEntity = {
            id: 'reference',
            revision: { version: 1 },
            permissions: deniedPermissions,
            operatePermissions: deniedPermissions
        };

        expect(deniedBulletin.bulletin).toBeUndefined();
        expect(readableReference.component?.name).toBe('Processor');
        expect(deniedReference.component).toBeUndefined();
        expectTypeOf<ControllerServiceReferencingComponentEntity>().not.toHaveProperty('uri');
    });

    it('requires status snapshot bodies even when their contents are not readable', () => {
        const deniedSnapshots: [
            ProcessorStatusSnapshotEntity,
            ConnectionStatusSnapshotEntity,
            PortStatusSnapshotEntity,
            ProcessGroupStatusSnapshotEntity,
            RemoteProcessGroupStatusSnapshotEntity
        ] = [
            { id: 'processor', canRead: false, processorStatusSnapshot: processorSnapshot('processor') },
            { id: 'connection', canRead: false, connectionStatusSnapshot: connectionSnapshot('connection') },
            { id: 'port', canRead: false, portStatusSnapshot: portSnapshot('port') },
            { id: 'group', canRead: false, processGroupStatusSnapshot: processGroupSnapshot('group') },
            {
                id: 'remote-group',
                canRead: false,
                remoteProcessGroupStatusSnapshot: remoteProcessGroupSnapshot('remote-group')
            }
        ];

        expect(deniedSnapshots.map((snapshot) => snapshot.id)).toEqual([
            'processor',
            'connection',
            'port',
            'group',
            'remote-group'
        ]);
    });

    it('keeps nested DTO and status graphs strict', () => {
        const readableProcessor = createProcessorEntity(processor('processor'), processorStatus('processor'));
        const readableGroup = createProcessGroupEntity(processGroup('group'), processGroupStatus('group'));

        expect(readableProcessor.component?.config.descriptors?.['destination'].required).toBe(true);
        expect(
            readableGroup.status?.aggregateSnapshot.processorStatusSnapshots?.[0].processorStatusSnapshot
                ?.processingPerformanceStatus?.sessionCommitDuration
        ).toBe(4);
        expectTypeOf(readableProcessor.status?.aggregateSnapshot.taskCount).toEqualTypeOf<number | undefined>();
        expectTypeOf(readableGroup.component?.contents?.remoteProcessGroups).toEqualTypeOf<
            RemoteProcessGroupDTO[] | undefined
        >();
    });

    it('requires positions on concrete positionable DTOs only', () => {
        expectTypeOf<ProcessorDTO['position']>().toEqualTypeOf<typeof position>();
        expectTypeOf<PortDTO['position']>().toEqualTypeOf<typeof position>();
        expectTypeOf<ProcessGroupDTO['position']>().toEqualTypeOf<typeof position>();
        expectTypeOf<RemoteProcessGroupDTO['position']>().toEqualTypeOf<typeof position>();
        expectTypeOf<LabelDTO['position']>().toEqualTypeOf<typeof position>();
        expectTypeOf<FunnelDTO['position']>().toEqualTypeOf<typeof position>();
        expectTypeOf<ConnectionDTO['position']>().toEqualTypeOf<typeof position | undefined>();
    });

    it('preserves Java-vetted required response fields', () => {
        expectTypeOf<LabelDTO['style']>().toEqualTypeOf<Record<string, string>>();
        expectTypeOf<LabelDTO['width']>().toEqualTypeOf<number>();
        expectTypeOf<LabelDTO['height']>().toEqualTypeOf<number>();
        expectTypeOf<LabelDTO['zIndex']>().toEqualTypeOf<number>();
        expectTypeOf<ProcessorStatusDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<ProcessorStatusSnapshotDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<ProcessorStatusSnapshotDTO['type']>().toEqualTypeOf<string>();
        expectTypeOf<ConnectionStatusDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<ConnectionStatusDTO['sourceName']>().toEqualTypeOf<string>();
        expectTypeOf<ConnectionStatusDTO['destinationName']>().toEqualTypeOf<string>();
        expectTypeOf<ConnectionStatusSnapshotDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<ConnectionStatusSnapshotDTO['sourceName']>().toEqualTypeOf<string>();
        expectTypeOf<ConnectionStatusSnapshotDTO['destinationName']>().toEqualTypeOf<string>();
        expectTypeOf<PortStatusDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<PortStatusSnapshotDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<ProcessGroupStatusDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<ProcessGroupStatusSnapshotDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<RemoteProcessGroupStatusDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<RemoteProcessGroupStatusSnapshotDTO['name']>().toEqualTypeOf<string>();
        expectTypeOf<AllowableValueEntity['allowableValue']>().toEqualTypeOf<AllowableValueDTO>();
        expectTypeOf<VersionedProcessGroup['statelessFlowFileContentInMemoryMax']>().toEqualTypeOf<
            string | undefined
        >();
        expectTypeOf<VersionedProcessGroup['statelessFlowFileContentInMemoryHeapPercentage']>().toEqualTypeOf<
            number | undefined
        >();
        expectTypeOf<ProcessGroupEntity['processGroupUpdateStrategy']>().toEqualTypeOf<
            'CURRENT_GROUP' | 'CURRENT_GROUP_WITH_CHILDREN' | undefined
        >();
    });
});
