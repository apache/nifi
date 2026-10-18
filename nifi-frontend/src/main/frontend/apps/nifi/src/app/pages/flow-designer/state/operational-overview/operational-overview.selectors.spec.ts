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

import { ComponentType, BulletinEntity } from '@nifi/shared';
import { ComponentEntity, ConnectionEntity, FlowState } from '../flow';
import { initialState } from '../flow/flow.reducer';
import { selectOperationalSelection, summarizeOperations } from './operational-overview.selectors';

function processor(id: string, runStatus?: string) {
    return {
        id,
        revision: { version: 0 },
        position: { x: 0, y: 0 },
        permissions: { canRead: true, canWrite: false },
        component: { name: id, state: runStatus?.toUpperCase() },
        status: { aggregateSnapshot: { runStatus } }
    };
}

function connection(id: string, snapshot: Record<string, number> = { flowFilesQueued: 0, bytesQueued: 0 }) {
    return {
        ...processor(id),
        sourceId: 'source',
        sourceGroupId: 'group-a',
        sourceType: 'PROCESSOR',
        destinationId: 'destination',
        destinationGroupId: 'group-a',
        destinationType: 'PROCESSOR',
        component: { name: id, backPressureObjectThreshold: 10000 },
        status: { aggregateSnapshot: snapshot }
    };
}

function group(processors: ComponentEntity[] = [], connections: ConnectionEntity[] = []): FlowState {
    const state = structuredClone(initialState);
    state.status = 'complete';
    state.flow.permissions.canRead = true;
    state.flow.processGroupFlow.id = 'group-a';
    state.flow.processGroupFlow.parentGroupId = 'parent';
    state.flow.processGroupFlow.breadcrumb.breadcrumb.name = 'Test group';
    state.flow.processGroupFlow.lastRefreshed = '14:00:00 ICT';
    state.flow.processGroupFlow.flow.processors = processors;
    state.flow.processGroupFlow.flow.connections = connections;
    return state;
}

describe('Operational overview selectors', () => {
    it('resolves the root route when its parent id is null or omitted without showing a retained child group', () => {
        const state = group([processor('root processor', 'Running')]);
        expect(summarizeOperations(state, 'root')).toMatchObject({ status: 'loading', visibleProcessors: 0 });
        state.flow.processGroupFlow.parentGroupId = null;
        expect(summarizeOperations(state, 'root')).toMatchObject({ status: 'ready', visibleProcessors: 1 });
        Reflect.deleteProperty(state.flow.processGroupFlow, 'parentGroupId');
        expect(summarizeOperations(state, 'root')).toMatchObject({ status: 'ready', visibleProcessors: 1 });
    });

    it('marks retained data stale on refresh failure but does not leak the previous group on failed navigation', () => {
        const state = group([processor('old')]);
        state.loadError = { id: 'group-a', status: 503 };
        expect(summarizeOperations(state, 'group-a')).toMatchObject({ status: 'ready', refreshFailed: true });
        state.loadError = { id: 'group-b', status: 503 };
        expect(summarizeOperations(state, 'group-b')).toMatchObject({ status: 'error', visibleProcessors: 0 });
        state.loadError = { id: 'group-a', status: 403 };
        expect(summarizeOperations(state, 'group-a')).toMatchObject({ status: 'forbidden', visibleProcessors: 0 });
    });

    it('counts only direct readable processors using reported states', () => {
        const entries = ['Running', 'Stopped', 'Invalid', 'Disabled', 'Validating'].map((status) =>
            processor(status, status)
        );
        const state = group([
            ...entries,
            { ...processor('restricted', 'Running'), permissions: { canRead: false, canWrite: false } }
        ]);
        state.flow.processGroupFlow.flow.processGroups = [{ ...processor('child'), component: { runningCount: 900 } }];
        const model = summarizeOperations(state, 'group-a');
        expect(model.processors).toEqual({ running: 1, stopped: 1, invalid: 1, disabled: 1, unknown: 1 });
        expect(model.visibleProcessors).toBe(5);
        expect(model.restricted).toBe(true);
    });

    it('prioritizes disabled over invalid validation and recognizes invalid stopped configuration', () => {
        const disabled = {
            ...processor('disabled', 'Disabled'),
            component: { state: 'DISABLED', validationStatus: 'INVALID' }
        };
        const invalid = { ...processor('invalid'), component: { state: 'STOPPED', validationStatus: 'INVALID' } };
        expect(summarizeOperations(group([disabled, invalid]), 'group-a').processors).toEqual({
            running: 0,
            stopped: 0,
            invalid: 1,
            disabled: 1,
            unknown: 0
        });
    });

    it('uses API node-max utilization instead of dividing cluster totals by one threshold', () => {
        const model = summarizeOperations(
            group(
                [],
                [
                    connection('clustered', {
                        flowFilesQueued: 20000,
                        bytesQueued: 500,
                        percentUseCount: 95,
                        percentUseBytes: 20
                    }),
                    connection('bytes', {
                        flowFilesQueued: 1,
                        bytesQueued: 1000,
                        percentUseCount: 1,
                        percentUseBytes: 98
                    })
                ]
            ),
            'group-a'
        );
        expect(model.queuedFlowFiles).toBe(20001);
        expect(model.highestUtilization).toEqual({ id: 'bytes', name: 'bytes', percent: 98 });
        expect(model.measuredConnections).toBe(2);
    });

    it('does not turn missing or disabled utilization into zero', () => {
        const model = summarizeOperations(group([], [connection('unmeasured')]), 'group-a');
        expect(model.queuedFlowFiles).toBe(0);
        expect(model.highestUtilization).toBeNull();
        expect(model.measuredConnections).toBe(0);
    });

    it('does not turn partial queue snapshots into complete totals', () => {
        const model = summarizeOperations(group([], [connection('partial', { flowFilesQueued: 4 })]), 'group-a');
        expect(model.queuedFlowFiles).toBe(4);
        expect(model.queuedBytes).toBeNull();
    });

    it('rejects nonfinite and negative queue observations', () => {
        const model = summarizeOperations(
            group(
                [],
                [
                    connection('bad', {
                        flowFilesQueued: Number.NaN,
                        bytesQueued: -1,
                        percentUseCount: Number.NaN,
                        percentUseBytes: -2
                    })
                ]
            ),
            'group-a'
        );
        expect(model.queuedFlowFiles).toBeNull();
        expect(model.queuedBytes).toBeNull();
        expect(model.highestUtilization).toBeNull();
    });

    it('distinguishes a loaded empty group from initial loading and forbidden access', () => {
        expect(summarizeOperations(initialState, null).status).toBe('loading');
        const state = group();
        expect(summarizeOperations(state, 'group-a').queuedFlowFiles).toBe(0);
        state.flow.permissions.canRead = false;
        const forbidden = summarizeOperations(state, 'group-a');
        expect(forbidden.status).toBe('forbidden');
        expect(forbidden.queuedFlowFiles).toBeNull();
        expect(forbidden.name).not.toBe('Test group');
    });

    it('hides an old group snapshot while navigating to a different group', () => {
        const state = group([processor('source', 'Running')]);
        state.status = 'loading';
        expect(summarizeOperations(state, 'group-b').status).toBe('loading');
        expect(summarizeOperations(state, 'group-b').visibleProcessors).toBe(0);
        expect(summarizeOperations(state, 'group-a').refreshing).toBe(true);
    });

    it('omits restricted connection metrics and bulletin contents', () => {
        const hidden = {
            ...connection('hidden', { flowFilesQueued: 9000, bytesQueued: 100, percentUseCount: 99 }),
            permissions: { canRead: false, canWrite: false }
        };
        const state = group([], [hidden]);
        state.controllerBulletins.bulletins = [
            { id: 1, canRead: false, bulletin: { message: 'restricted message', level: 'ERROR' } },
            { id: 2, canRead: true, bulletin: { message: 'visible message', level: 'WARN' } }
        ] as BulletinEntity[];
        const model = summarizeOperations(state, 'group-a');
        expect(model.visibleConnections).toBe(0);
        expect(model.highestUtilization).toBeNull();
        expect(model.controllerBulletins).toEqual([{ message: 'visible message', level: 'WARN' }]);
    });

    it('does not expose selected metadata after permission is revoked', () => {
        const entity = {
            ...connection('secret', { flowFilesQueued: 9000 }),
            permissions: { canRead: false, canWrite: false }
        };
        const state = group([], [entity]);
        const selection = selectOperationalSelection.projector(
            state,
            { id: 'secret', componentType: ComponentType.Connection },
            summarizeOperations(state, 'group-a')
        );
        expect(selection).toEqual({ id: 'secret', kind: 'connection', readable: false, name: 'Restricted component' });
    });

    it('clears the inspector for removed, unsupported or absent selection', () => {
        const state = group();
        const model = summarizeOperations(state, 'group-a');
        expect(selectOperationalSelection.projector(state, null, model)).toBeNull();
        expect(
            selectOperationalSelection.projector(
                state,
                { id: 'missing', componentType: ComponentType.Connection },
                model
            )
        ).toBeNull();
        expect(
            selectOperationalSelection.projector(
                state,
                { id: 'group', componentType: ComponentType.ProcessGroup },
                model
            )
        ).toBeNull();
    });
});
