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

import { buildComponentIdToNameMap, collectEndpointGroupIds } from './component-connections.utils';
import { ComponentEntity, ConnectionEntity, ProcessGroupFlowEntity } from './index';

const DEFINING_GROUP_ID = 'defining-group-id';
const CHILD_GROUP_ID = 'child-group-id';
const OTHER_CHILD_GROUP_ID = 'other-child-group-id';
const REMOTE_GROUP_ID = 'remote-process-group-id';

interface EndpointOptions {
    sourceGroupId?: string;
    sourceType?: string;
    destinationGroupId?: string;
    destinationType?: string;
}

function connection(options: EndpointOptions = {}): ConnectionEntity {
    return {
        id: 'connection-id',
        permissions: { canRead: false, canWrite: false },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        sourceId: 'source-id',
        sourceGroupId: options.sourceGroupId ?? DEFINING_GROUP_ID,
        sourceType: options.sourceType ?? 'PROCESSOR',
        destinationId: 'destination-id',
        destinationGroupId: options.destinationGroupId ?? DEFINING_GROUP_ID,
        destinationType: options.destinationType ?? 'PROCESSOR',
        component: null
    };
}

function component(id: string, name: string | undefined, canRead: boolean): ComponentEntity {
    return {
        id,
        permissions: { canRead, canWrite: false },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        component: { id, name }
    };
}

function flowEntity(flow: Partial<ProcessGroupFlowEntity['processGroupFlow']['flow']>): ProcessGroupFlowEntity {
    return {
        permissions: { canRead: true, canWrite: true },
        processGroupFlow: {
            id: DEFINING_GROUP_ID,
            uri: '',
            parentGroupId: null,
            breadcrumb: {
                id: DEFINING_GROUP_ID,
                permissions: { canRead: true, canWrite: true },
                versionedFlowState: '',
                breadcrumb: { id: DEFINING_GROUP_ID, name: 'Defining Process Group' }
            },
            flow: {
                processGroups: [],
                remoteProcessGroups: [],
                processors: [],
                inputPorts: [],
                outputPorts: [],
                connections: [],
                labels: [],
                funnels: [],
                ...flow
            }
        }
    } as unknown as ProcessGroupFlowEntity;
}

describe('collectEndpointGroupIds', () => {
    it('collects nothing when both ends are in the group that defines the connections', () => {
        expect(collectEndpointGroupIds([connection()], DEFINING_GROUP_ID)).toEqual([]);
    });

    it('collects the group behind a port an end reaches into', () => {
        const connections = [
            connection({ destinationGroupId: CHILD_GROUP_ID, destinationType: 'INPUT_PORT' }),
            connection({ sourceGroupId: OTHER_CHILD_GROUP_ID, sourceType: 'OUTPUT_PORT' })
        ];

        expect(collectEndpointGroupIds(connections, DEFINING_GROUP_ID)).toEqual([CHILD_GROUP_ID, OTHER_CHILD_GROUP_ID]);
    });

    it('collects a group reached by several connections once', () => {
        const connections = [
            connection({ destinationGroupId: CHILD_GROUP_ID, destinationType: 'INPUT_PORT' }),
            connection({ destinationGroupId: CHILD_GROUP_ID, destinationType: 'INPUT_PORT' }),
            connection({ sourceGroupId: CHILD_GROUP_ID, sourceType: 'OUTPUT_PORT' })
        ];

        expect(collectEndpointGroupIds(connections, DEFINING_GROUP_ID)).toEqual([CHILD_GROUP_ID]);
    });

    it('leaves out a Remote Process Group, which has no flow of its own to load', () => {
        const connections = [
            connection({ sourceGroupId: REMOTE_GROUP_ID, sourceType: 'REMOTE_OUTPUT_PORT' }),
            connection({ destinationGroupId: REMOTE_GROUP_ID, destinationType: 'REMOTE_INPUT_PORT' })
        ];

        expect(collectEndpointGroupIds(connections, DEFINING_GROUP_ID)).toEqual([]);
    });
});

describe('buildComponentIdToNameMap', () => {
    it('names only the components the current user can read', () => {
        const flow = flowEntity({
            processors: [
                component('readable-processor-id', 'Readable Processor', true),
                component('hidden-processor-id', 'Hidden Processor', false)
            ]
        });

        const idToName = buildComponentIdToNameMap([flow]);

        expect(idToName.get('readable-processor-id')).toBe('Readable Processor');
        expect(idToName.has('hidden-processor-id')).toBeFalsy();
    });

    it('names components of every group it is given, which is what separates the two ends', () => {
        const definingFlow = flowEntity({
            processors: [component('hidden-processor-id', 'Hidden Processor', false)]
        });
        const childFlow = flowEntity({
            inputPorts: [component('input-port-id', 'Input Port A', true)]
        });

        const idToName = buildComponentIdToNameMap([definingFlow, childFlow]);

        expect(idToName.has('hidden-processor-id')).toBeFalsy();
        expect(idToName.get('input-port-id')).toBe('Input Port A');
    });

    it('names every kind of component a connection can be attached to', () => {
        const flow = flowEntity({
            processors: [component('processor-id', 'Processor', true)],
            inputPorts: [component('input-port-id', 'Input Port', true)],
            outputPorts: [component('output-port-id', 'Output Port', true)],
            funnels: [component('funnel-id', undefined, true)]
        });

        const idToName = buildComponentIdToNameMap([flow]);

        expect(idToName.get('processor-id')).toBe('Processor');
        expect(idToName.get('input-port-id')).toBe('Input Port');
        expect(idToName.get('output-port-id')).toBe('Output Port');
        // a funnel has no name of its own, and is still reported as readable
        expect(idToName.get('funnel-id')).toBe('');
    });

    it('builds an empty map when no flow could be loaded', () => {
        expect(buildComponentIdToNameMap([]).size).toBe(0);
    });
});
