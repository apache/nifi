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

import { ComponentType } from '@nifi/shared';
import type {
    ConnectionEntity,
    CreateRevisionRequest,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    RevisionRequest
} from '@nifi/shared';
import { describe, expect, expectTypeOf, it } from 'vitest';
import type {
    CanvasComponentEntity,
    CanvasComponentRef,
    CanvasConnectableEntity,
    ComponentRunStatusRequest,
    ConnectionCreateBody,
    ConnectionCreateRequest,
    ConnectionDestinationUpdate,
    ConnectionEndpointReconnectDestination,
    ConnectionGeometryUpdate,
    FunnelCreateRequest,
    LabelCreateBody,
    LabelCreateRequest,
    MovableComponentUpdate,
    PortCreateRequest,
    ProcessGroupCreateRequest,
    ProcessorCreateBody,
    ProcessorCreateRequest,
    RemoteProcessGroupCreateRequest,
    SnippetCreateRequest,
    SnippetDeleteRequest,
    SnippetMoveRequest,
    SnippetRevisionMaps
} from './index';

const revision: RevisionRequest = { version: 3, clientId: 'client-id' };
const createRevision: CreateRevisionRequest = { version: 0, clientId: 'client-id' };
const position = { x: 10, y: 20 };

describe('canvas contracts', () => {
    it('represents lightweight component and reconnect references', () => {
        const component: CanvasComponentRef = { id: 'processor', type: ComponentType.Processor };
        const entity = {} as ProcessorEntity;
        const destination: ConnectionEndpointReconnectDestination = {
            id: 'processor',
            componentType: ComponentType.Processor,
            entity
        };

        expect(component).toEqual({ id: 'processor', type: ComponentType.Processor });
        expect(destination.entity).toBe(entity);
    });

    it('keeps canvas and connectable entity unions discriminated', () => {
        expectTypeOf<ProcessorEntity>().toExtend<CanvasConnectableEntity>();
        expectTypeOf<PortEntity>().toExtend<CanvasConnectableEntity>();
        expectTypeOf<ProcessGroupEntity>().toExtend<CanvasConnectableEntity>();
        expectTypeOf<RemoteProcessGroupEntity>().toExtend<CanvasConnectableEntity>();
        expectTypeOf<FunnelEntity>().toExtend<CanvasConnectableEntity>();
        expectTypeOf<LabelEntity>().not.toExtend<CanvasConnectableEntity>();
        expectTypeOf<ConnectionEntity>().not.toExtend<CanvasConnectableEntity>();

        expectTypeOf<CanvasConnectableEntity>().toExtend<CanvasComponentEntity>();
        expectTypeOf<LabelEntity>().toExtend<CanvasComponentEntity>();
        expectTypeOf<ConnectionEntity>().toExtend<CanvasComponentEntity>();
    });
});

function snippetRevisionMaps(): SnippetRevisionMaps {
    return {
        processors: { processor: revision },
        inputPorts: { input: revision },
        outputPorts: { output: revision },
        processGroups: { group: revision },
        remoteProcessGroups: { remote: revision },
        funnels: { funnel: revision },
        labels: { label: revision },
        connections: { connection: revision }
    };
}

describe('flow write contracts', () => {
    it('represents all seven create request families', () => {
        const processor: ProcessorCreateRequest = {
            revision: createRevision,
            component: {
                position,
                type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
                bundle: { group: 'org.apache.nifi', artifact: 'nifi-standard-nar', version: '2.0.0' }
            }
        };
        const connection: ConnectionCreateRequest = {
            revision: createRevision,
            component: {
                labelIndex: 0,
                source: { id: 'source', groupId: 'root', type: 'PROCESSOR' },
                destination: { id: 'destination', groupId: 'root', type: 'INPUT_PORT' },
                bends: [position]
            }
        };
        const inputPort: PortCreateRequest = {
            revision: createRevision,
            component: { position, name: 'Input', allowRemoteAccess: false }
        };
        const outputPort: PortCreateRequest = {
            revision: createRevision,
            component: { position, name: 'Output', allowRemoteAccess: true }
        };
        const processGroup: ProcessGroupCreateRequest = {
            revision: createRevision,
            component: { position, name: 'Group', parameterContext: { id: 'parameter-context' } }
        };
        const remoteProcessGroup: RemoteProcessGroupCreateRequest = {
            revision: createRevision,
            component: {
                position,
                targetUris: 'https://example.test/nifi',
                transportProtocol: 'RAW',
                communicationsTimeout: '30 sec'
            }
        };
        const funnel: FunnelCreateRequest = { revision: createRevision, component: { position } };
        const label: LabelCreateRequest = {
            revision: createRevision,
            component: { position, zIndex: 1, label: 'Label', width: 200, height: 80 }
        };

        expect([
            processor,
            connection,
            inputPort,
            outputPort,
            processGroup,
            remoteProcessGroup,
            funnel,
            label
        ]).toHaveLength(8);
        expectTypeOf(inputPort.component).not.toHaveProperty('type');
        expectTypeOf(outputPort.component).not.toHaveProperty('type');
    });

    it('requires version zero for create revisions', () => {
        const valid: ProcessorCreateRequest = {
            revision: { version: 0 },
            component: {
                position,
                type: 'processor',
                bundle: { group: 'group', artifact: 'artifact', version: '1' }
            }
        };

        const missingVersion: ProcessorCreateRequest = {
            // @ts-expect-error Create endpoints require an explicit revision version.
            revision: {},
            component: valid.component
        };
        const nonzeroVersion: ProcessorCreateRequest = {
            // @ts-expect-error Create endpoints require revision version zero.
            revision: { version: 1 },
            component: valid.component
        };

        expect(valid.revision.version).toBe(0);
        expect((missingVersion.revision as RevisionRequest).version).toBeUndefined();
        expect((nonzeroVersion.revision as RevisionRequest).version).toBe(1);
    });

    it('represents component update contracts', () => {
        const movable: MovableComponentUpdate = {
            id: 'processor',
            position,
            type: ComponentType.Processor,
            revision
        };
        const geometry: ConnectionGeometryUpdate = {
            id: 'connection',
            labelIndex: 1,
            bends: [position],
            revision
        };
        const destination: ConnectionDestinationUpdate = {
            id: 'connection',
            revision,
            destination: { id: 'port', groupId: 'root', type: 'INPUT_PORT' }
        };
        const runStatus: ComponentRunStatusRequest = {
            state: 'RUNNING',
            revision,
            disconnectedNodeAcknowledged: true
        };

        expect(movable.type).toBe(ComponentType.Processor);
        expect(geometry.bends).toEqual([position]);
        expect(destination.destination.id).toBe('port');
        expect(runStatus.state).toBe('RUNNING');
    });

    it('represents snippet create, move, and delete requests', () => {
        const create: SnippetCreateRequest = {
            disconnectedNodeAcknowledged: false,
            snippet: { parentGroupId: 'root', ...snippetRevisionMaps() }
        };
        const move: SnippetMoveRequest = {
            disconnectedNodeAcknowledged: true,
            snippet: { id: 'snippet', parentGroupId: 'destination-group' }
        };
        const remove: SnippetDeleteRequest = {
            id: 'snippet',
            options: { params: { disconnectedNodeAcknowledged: true } }
        };

        expect(Object.keys(create.snippet.connections)).toEqual(['connection']);
        expect(move.snippet.parentGroupId).toBe('destination-group');
        expect(remove.options.params.disconnectedNodeAcknowledged).toBe(true);
    });

    it('keeps request revisions and bodies separate from response fields', () => {
        expectTypeOf<RevisionRequest>().toHaveProperty('version').toEqualTypeOf<number | undefined>();
        expectTypeOf<RevisionRequest>().toHaveProperty('clientId').toEqualTypeOf<string | undefined>();
        expectTypeOf<RevisionRequest>().not.toHaveProperty('lastModifier');
        expectTypeOf<ProcessorCreateBody>().not.toHaveProperty('permissions');
        expectTypeOf<ProcessorCreateBody>().not.toHaveProperty('status');
        expectTypeOf<ConnectionCreateBody>().not.toHaveProperty('permissions');
        expectTypeOf<ConnectionCreateBody>().not.toHaveProperty('status');
        expectTypeOf<LabelCreateBody>().not.toHaveProperty('dimensions');

        // @ts-expect-error lastModifier is response-only revision metadata.
        const responseRevision: RevisionRequest = { version: 1, lastModifier: 'user' };
        const processorWithPermissions: ProcessorCreateBody = {
            position,
            type: 'processor',
            bundle: { group: 'group', artifact: 'artifact', version: '1' },
            // @ts-expect-error permissions are response envelope fields.
            permissions: { canRead: true, canWrite: true }
        };
        const connectionWithStatus: ConnectionCreateBody = {
            labelIndex: 0,
            source: { id: 'source', groupId: 'root', type: 'PROCESSOR' },
            destination: { id: 'destination', groupId: 'root', type: 'INPUT_PORT' },
            // @ts-expect-error status is not accepted by create request bodies.
            status: {}
        };

        expect((responseRevision as unknown as { lastModifier: string }).lastModifier).toBe('user');
        expect(
            (processorWithPermissions as unknown as { permissions: { canWrite: boolean } }).permissions.canWrite
        ).toBe(true);
        expect((connectionWithStatus as unknown as { status: object }).status).toEqual({});
    });
});
