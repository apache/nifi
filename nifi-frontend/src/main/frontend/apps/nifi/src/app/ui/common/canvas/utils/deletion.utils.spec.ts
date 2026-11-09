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
import { describe, expect, it } from 'vitest';
import { SelectionTarget } from './selection.utils';
import { areDeletable, isDeletable } from './deletion.utils';

const PG = 'pg-current';

interface Endpoint {
    type?: string;
    groupId?: string;
    running?: boolean;
}

function runnable(
    componentType: ComponentType.Processor | ComponentType.InputPort | ComponentType.OutputPort,
    runStatus = 'Stopped',
    activeThreadCount = 0,
    canWrite = true
): SelectionTarget {
    return {
        entity: {
            id: `${componentType}-1`,
            permissions: { canRead: true, canWrite },
            revision: { version: 1 },
            status: { aggregateSnapshot: { runStatus, activeThreadCount } },
            component: {}
        },
        ui: { componentType }
    } as SelectionTarget;
}

function remoteProcessGroup(transmissionStatus = 'NotTransmitting', activeThreadCount = 0): SelectionTarget {
    return {
        entity: {
            id: 'rpg-1',
            permissions: { canRead: true, canWrite: true },
            revision: { version: 1 },
            status: { transmissionStatus, aggregateSnapshot: { activeThreadCount } }
        },
        ui: { componentType: ComponentType.RemoteProcessGroup }
    } as SelectionTarget;
}

function simple(
    componentType: ComponentType.ProcessGroup | ComponentType.Funnel | ComponentType.Label
): SelectionTarget {
    return {
        entity: { id: `${componentType}-1`, permissions: { canRead: true, canWrite: true }, revision: { version: 1 } },
        ui: { componentType }
    } as SelectionTarget;
}

function connection(source: Endpoint, destination: Endpoint, readable = true): SelectionTarget {
    const endpoint = ({ type = 'PROCESSOR', groupId = PG, running = false }: Endpoint) => ({
        id: `${type}-endpoint`,
        type,
        groupId,
        name: 'endpoint',
        running
    });
    return {
        entity: {
            id: 'connection-1',
            permissions: { canRead: readable, canWrite: true },
            revision: { version: 1 },
            ...(readable ? { component: { source: endpoint(source), destination: endpoint(destination) } } : {})
        },
        ui: { componentType: ComponentType.Connection }
    } as SelectionTarget;
}

describe('deletion utilities', () => {
    it('requires write permission', () => {
        expect(isDeletable(runnable(ComponentType.Processor, 'Stopped', 0, false), PG)).toBe(false);
    });

    it('allows stopped runnables and rejects running or active ones', () => {
        expect(isDeletable(runnable(ComponentType.Processor), PG)).toBe(true);
        expect(isDeletable(runnable(ComponentType.InputPort, 'Running'), PG)).toBe(false);
        expect(isDeletable(runnable(ComponentType.OutputPort, 'Stopped', 1), PG)).toBe(false);
    });

    it('rejects a transmitting or active remote process group', () => {
        expect(isDeletable(remoteProcessGroup(), PG)).toBe(true);
        expect(isDeletable(remoteProcessGroup('Transmitting'), PG)).toBe(false);
        expect(isDeletable(remoteProcessGroup('NotTransmitting', 2), PG)).toBe(false);
    });

    it('allows writable process groups, funnels, and labels', () => {
        expect(isDeletable(simple(ComponentType.ProcessGroup), PG)).toBe(true);
        expect(isDeletable(simple(ComponentType.Funnel), PG)).toBe(true);
        expect(isDeletable(simple(ComponentType.Label), PG)).toBe(true);
    });

    it('rejects a connection with a running local endpoint', () => {
        expect(isDeletable(connection({}, {}), PG)).toBe(true);
        expect(isDeletable(connection({ running: true }, {}), PG)).toBe(false);
        expect(isDeletable(connection({}, { running: true }), PG)).toBe(false);
    });

    it('allows a connection to or from a running local funnel', () => {
        expect(isDeletable(connection({ type: 'FUNNEL', running: true }, {}), PG)).toBe(true);
        expect(isDeletable(connection({}, { type: 'FUNNEL', running: true }), PG)).toBe(true);
    });

    it('treats child and remote process group endpoints as configurable', () => {
        expect(isDeletable(connection({ type: 'OUTPUT_PORT', groupId: 'pg-child', running: true }, {}), PG)).toBe(true);
        expect(isDeletable(connection({}, { type: 'REMOTE_INPUT_PORT', groupId: 'rpg-1', running: true }), PG)).toBe(
            true
        );
    });

    it('defers to the server for an unreadable connection', () => {
        expect(isDeletable(connection({}, {}, false), PG)).toBe(true);
    });

    it('requires a non-empty selection where every target is deletable', () => {
        expect(areDeletable([], PG)).toBe(false);
        expect(areDeletable([simple(ComponentType.Funnel), runnable(ComponentType.Processor)], PG)).toBe(true);
        expect(areDeletable([simple(ComponentType.Funnel), runnable(ComponentType.Processor, 'Running')], PG)).toBe(
            false
        );
    });
});
