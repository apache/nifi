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

import { connectorCanvasReducer } from './connector-canvas.reducer';
import { ConnectorCanvasState, initialConnectorCanvasState } from './index';
import * as ConnectorCanvasActions from './connector-canvas.actions';
import { ErrorContextKey } from '../../../../state/error';
import { createParameterContextFixture } from '../../testing/parameter-context-fixture';
import { ComponentType } from '@nifi/shared';
import {
    makeConnection,
    makeFunnel,
    makeLabel,
    makePort,
    makeProcessGroup,
    makeProcessor,
    makeRemoteProcessGroup
} from '../../testing/connector-canvas-entity-fixtures';

describe('connectorCanvasReducer', () => {
    function createPopulatedState(overrides: Partial<ConnectorCanvasState> = {}): ConnectorCanvasState {
        return {
            ...initialConnectorCanvasState,
            connectorId: 'connector-1',
            processGroupId: 'pg-current',
            parentProcessGroupId: 'pg-parent',
            breadcrumb: {
                id: 'pg-current',
                permissions: { canRead: true, canWrite: false },
                breadcrumb: { id: 'pg-current', name: 'Current PG' },
                versionedFlowState: ''
            },
            labels: [makeLabel('label-1')],
            funnels: [makeFunnel('funnel-1')],
            inputPorts: [makePort('input-1')],
            outputPorts: [makePort('output-1', { portType: 'OUTPUT_PORT' })],
            remoteProcessGroups: [makeRemoteProcessGroup('rpg-1')],
            processGroups: [makeProcessGroup('pg-child-1')],
            processors: [makeProcessor('proc-1'), makeProcessor('proc-2')],
            connections: [makeConnection('conn-1')],
            registryClients: [],
            skipTransform: false,
            hasLoadedFlow: true,
            loadingStatus: 'success',
            error: null,
            ...overrides
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadConnectorFlow', () => {
        it('should clear component data when navigating to a different process group', () => {
            const previousState = createPopulatedState({ processGroupId: 'pg-current' });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlow({ connectorId: 'connector-1', processGroupId: 'pg-different' })
            );

            expect(result.processGroupId).toBe('pg-different');
            expect(result.loadingStatus).toBe('loading');
            expect(result.hasLoadedFlow).toBe(false);
            expect(result.error).toBeNull();
            expect(result.labels).toEqual([]);
            expect(result.funnels).toEqual([]);
            expect(result.inputPorts).toEqual([]);
            expect(result.outputPorts).toEqual([]);
            expect(result.remoteProcessGroups).toEqual([]);
            expect(result.processGroups).toEqual([]);
            expect(result.processors).toEqual([]);
            expect(result.connections).toEqual([]);
        });

        it('should preserve component data when refreshing the same process group', () => {
            const previousState = createPopulatedState({ processGroupId: 'pg-current' });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlow({ connectorId: 'connector-1', processGroupId: 'pg-current' })
            );

            expect(result.processGroupId).toBe('pg-current');
            expect(result.loadingStatus).toBe('loading');
            expect(result.error).toBeNull();
            expect(result.processors).toEqual(previousState.processors);
            expect(result.connections).toEqual(previousState.connections);
            expect(result.labels).toEqual(previousState.labels);
            expect(result.funnels).toEqual(previousState.funnels);
            expect(result.inputPorts).toEqual(previousState.inputPorts);
            expect(result.outputPorts).toEqual(previousState.outputPorts);
            expect(result.remoteProcessGroups).toEqual(previousState.remoteProcessGroups);
            expect(result.processGroups).toEqual(previousState.processGroups);
        });

        it('should clear component data when navigating to a different connector with the same process group id', () => {
            const previousState = createPopulatedState({
                connectorId: 'connector-1',
                processGroupId: 'pg-current'
            });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlow({ connectorId: 'connector-2', processGroupId: 'pg-current' })
            );

            expect(result.connectorId).toBe('connector-2');
            expect(result.processGroupId).toBe('pg-current');
            expect(result.labels).toEqual([]);
            expect(result.funnels).toEqual([]);
            expect(result.inputPorts).toEqual([]);
            expect(result.outputPorts).toEqual([]);
            expect(result.remoteProcessGroups).toEqual([]);
            expect(result.processGroups).toEqual([]);
            expect(result.processors).toEqual([]);
            expect(result.connections).toEqual([]);
            expect(result.parameterContext).toBeNull();
        });
    });

    describe('loadConnectorFlowSuccess', () => {
        it('should populate state with flow data', () => {
            const processors = [makeProcessor('proc-new')];
            const connections = [makeConnection('conn-new')];
            const labels = [makeLabel('label-new')];

            const result = connectorCanvasReducer(
                initialConnectorCanvasState,
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    connectorId: 'connector-1',
                    processGroupId: 'pg-1',
                    parentProcessGroupId: 'pg-parent',
                    breadcrumb: null,
                    labels,
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors,
                    connections
                })
            );

            expect(result.loadingStatus).toBe('success');
            expect(result.connectorId).toBe('connector-1');
            expect(result.processGroupId).toBe('pg-1');
            expect(result.parentProcessGroupId).toBe('pg-parent');
            expect(result.processors).toEqual(processors);
            expect(result.connections).toEqual(connections);
            expect(result.labels).toEqual(labels);
            expect(result.error).toBeNull();
        });

        it('should preserve a newer local component when an older poll response arrives', () => {
            const current = makeProcessor('proc-1', { revision: { version: 2 } });
            const stale = makeProcessor('proc-1', { revision: { version: 1 } });

            const result = connectorCanvasReducer(
                createPopulatedState({ processors: [current], saving: true }),
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    connectorId: 'connector-1',
                    processGroupId: 'pg-current',
                    parentProcessGroupId: 'pg-parent',
                    breadcrumb: null,
                    labels: [],
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors: [stale],
                    connections: []
                })
            );

            expect(result.processors).toEqual([current]);
            expect(result.saving).toBe(true);
        });

        it('should retain a locally created connection until a snapshot confirms it', () => {
            const connection = makeConnection('conn-created', { revision: { version: 0 } });
            const state = createPopulatedState({
                connections: [connection],
                addedCache: [connection.id]
            });

            const staleAction = ConnectorCanvasActions.loadConnectorFlowSuccess({
                connectorId: 'connector-1',
                processGroupId: 'pg-current',
                parentProcessGroupId: 'pg-parent',
                breadcrumb: null,
                labels: [],
                funnels: [],
                inputPorts: [],
                outputPorts: [],
                remoteProcessGroups: [],
                processGroups: [],
                processors: [],
                connections: []
            });
            const afterFirstStaleLoad = connectorCanvasReducer(state, staleAction);
            const afterSecondStaleLoad = connectorCanvasReducer(afterFirstStaleLoad, staleAction);

            expect(afterSecondStaleLoad.connections).toEqual([connection]);
            expect(afterSecondStaleLoad.addedCache).toEqual([connection.id]);

            const confirmed = connectorCanvasReducer(
                afterSecondStaleLoad,
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    ...staleAction,
                    connections: [makeConnection('conn-created', { revision: { version: 1 } })]
                })
            );

            expect(confirmed.addedCache).toEqual([]);
        });

        it('should retain a removed component tombstone until a snapshot confirms its absence', () => {
            const state = createPopulatedState({
                processors: [],
                removedCache: ['proc-removed']
            });
            const staleProcessor = makeProcessor('proc-removed');
            const staleAction = ConnectorCanvasActions.loadConnectorFlowSuccess({
                connectorId: 'connector-1',
                processGroupId: 'pg-current',
                parentProcessGroupId: 'pg-parent',
                breadcrumb: null,
                labels: [],
                funnels: [],
                inputPorts: [],
                outputPorts: [],
                remoteProcessGroups: [],
                processGroups: [],
                processors: [staleProcessor],
                connections: []
            });

            const afterFirstStaleLoad = connectorCanvasReducer(state, staleAction);
            const afterSecondStaleLoad = connectorCanvasReducer(afterFirstStaleLoad, staleAction);

            expect(afterSecondStaleLoad.processors).toEqual([]);
            expect(afterSecondStaleLoad.removedCache).toEqual(['proc-removed']);

            const confirmed = connectorCanvasReducer(
                afterSecondStaleLoad,
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    ...staleAction,
                    processors: []
                })
            );

            expect(confirmed.removedCache).toEqual([]);
        });

        it('should suppress stale connections attached to a removed component', () => {
            const connection = makeConnection('conn-attached', {
                sourceId: 'proc-removed',
                destinationId: 'proc-other'
            });

            const result = connectorCanvasReducer(
                createPopulatedState({
                    processors: [makeProcessor('proc-other')],
                    connections: [],
                    removedCache: ['proc-removed']
                }),
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    connectorId: 'connector-1',
                    processGroupId: 'pg-current',
                    parentProcessGroupId: 'pg-parent',
                    breadcrumb: null,
                    labels: [],
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors: [makeProcessor('proc-other')],
                    connections: [connection]
                })
            );

            expect(result.connections).toEqual([]);
            expect(result.removedCache).toContain('proc-removed');
        });
    });

    describe('loadConnectorFlowFailure', () => {
        it('should show a fatal error when the initial load fails', () => {
            const previousState = createPopulatedState({ hasLoadedFlow: false, loadingStatus: 'loading' });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlowFailure({
                    errorContext: {
                        context: ErrorContextKey.CONNECTORS,
                        errors: ['Connection refused']
                    },
                    isRefresh: false
                })
            );

            expect(result.loadingStatus).toBe('error');
            expect(result.error).toBe('Connection refused');
            expect(result.connectorId).toBe(previousState.connectorId);
            expect(result.processGroupId).toBe(previousState.processGroupId);
            expect(result.processors).toEqual(previousState.processors);
            expect(result.connections).toEqual(previousState.connections);
        });

        it('should preserve a loaded canvas when a refresh fails', () => {
            const previousState = createPopulatedState({ loadingStatus: 'loading' });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlowFailure({
                    errorContext: {
                        context: ErrorContextKey.CONNECTORS,
                        errors: ['Service unavailable']
                    },
                    isRefresh: true
                })
            );

            expect(result.loadingStatus).toBe('refresh-error');
            expect(result.hasLoadedFlow).toBe(true);
            expect(result.processors).toEqual(previousState.processors);
        });
    });

    describe('createConnection', () => {
        it('adds a created connection and clears saving', () => {
            const connection = makeConnection('conn-new', { revision: { version: 0, clientId: 'local-client' } });

            const saving = connectorCanvasReducer(
                createPopulatedState(),
                ConnectorCanvasActions.submitCreateConnection({
                    processGroupId: 'pg-current',
                    request: { payload: connection.component as never }
                })
            );
            const result = connectorCanvasReducer(
                saving,
                ConnectorCanvasActions.createConnectionSuccess({ connection })
            );

            expect(saving.saving).toBe(true);
            expect(result.saving).toBe(false);
            expect(result.connections).toContainEqual(connection);
        });

        it('does not replace a newer connection with a stale create response', () => {
            const current = makeConnection('conn-1', { revision: { version: 2 }, zIndex: 7 });
            const stale = makeConnection('conn-1', { revision: { version: 1 }, zIndex: 3 });

            const result = connectorCanvasReducer(
                createPopulatedState({ connections: [current], saving: true }),
                ConnectorCanvasActions.createConnectionSuccess({ connection: stale })
            );

            expect(result.saving).toBe(false);
            expect(result.connections).toEqual([current]);
        });

        it('marks a dialog reconnect save as saving', () => {
            const result = connectorCanvasReducer(
                createPopulatedState(),
                ConnectorCanvasActions.saveEditedConnection({
                    request: {
                        id: 'conn-1',
                        type: ComponentType.Connection,
                        uri: '/connections/conn-1',
                        payload: { revision: { version: 1 }, component: { id: 'conn-1' } },
                        errorStrategy: 'banner'
                    }
                })
            );

            expect(result.saving).toBe(true);
        });

        it('clears saving when creation fails', () => {
            const result = connectorCanvasReducer(
                createPopulatedState({ saving: true }),
                ConnectorCanvasActions.createConnectionFailure({ error: 'failed' })
            );

            expect(result.saving).toBe(false);
        });
    });

    describe('resetConnectorCanvasState', () => {
        it('should reset to initial state', () => {
            const previousState = createPopulatedState();

            const result = connectorCanvasReducer(previousState, ConnectorCanvasActions.resetConnectorCanvasState());

            expect(result).toEqual(initialConnectorCanvasState);
        });
    });

    describe('navigateWithoutTransform', () => {
        it('should set skipTransform to true', () => {
            const previousState = createPopulatedState({ skipTransform: false });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.navigateWithoutTransform({
                    url: ['/connectors', 'conn-1', 'canvas', 'pg-1', 'Processor', 'proc-1']
                })
            );

            expect(result.skipTransform).toBe(true);
        });
    });

    describe('setSkipTransform', () => {
        it('should set skipTransform to the provided value', () => {
            const previousState = createPopulatedState({ skipTransform: true });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.setSkipTransform({ skipTransform: false })
            );

            expect(result.skipTransform).toBe(false);
        });

        it('should set skipTransform to true', () => {
            const previousState = createPopulatedState({ skipTransform: false });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.setSkipTransform({ skipTransform: true })
            );

            expect(result.skipTransform).toBe(true);
        });
    });

    describe('parameter context', () => {
        const sampleParameterContext = createParameterContextFixture();

        it('should clear parameter context when navigating to a different process group', () => {
            const previousState = createPopulatedState({
                processGroupId: 'pg-current',
                parameterContext: sampleParameterContext
            });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlow({ connectorId: 'connector-1', processGroupId: 'pg-other' })
            );

            expect(result.parameterContext).toBeNull();
        });

        it('should preserve parameter context when refreshing the same process group', () => {
            const previousState = createPopulatedState({
                processGroupId: 'pg-current',
                parameterContext: sampleParameterContext
            });

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlow({ connectorId: 'connector-1', processGroupId: 'pg-current' })
            );

            expect(result.parameterContext).toBe(sampleParameterContext);
        });

        it('should store the parameter context on load success', () => {
            const result = connectorCanvasReducer(
                createPopulatedState(),
                ConnectorCanvasActions.loadConnectorParameterContextSuccess({
                    parameterContext: sampleParameterContext
                })
            );

            expect(result.parameterContext).toBe(sampleParameterContext);
        });

        it('should store a null parameter context on load success', () => {
            const result = connectorCanvasReducer(
                createPopulatedState({ parameterContext: sampleParameterContext }),
                ConnectorCanvasActions.loadConnectorParameterContextSuccess({ parameterContext: null })
            );

            expect(result.parameterContext).toBeNull();
        });

        it('should reset parameter context on load failure', () => {
            const result = connectorCanvasReducer(
                createPopulatedState({ parameterContext: sampleParameterContext }),
                ConnectorCanvasActions.loadConnectorParameterContextFailure({
                    errorContext: { context: ErrorContextKey.CONNECTORS, errors: ['boom'] }
                })
            );

            expect(result.parameterContext).toBeNull();
        });
    });

    describe('canvas mutations', () => {
        it('tracks saving through bulk completion', () => {
            const saving = connectorCanvasReducer(
                createPopulatedState(),
                ConnectorCanvasActions.updateComponentPositionsBulk({
                    processGroupId: 'pg-current',
                    updates: [],
                    delta: { x: 1, y: 2 }
                })
            );
            const complete = connectorCanvasReducer(
                saving,
                ConnectorCanvasActions.updateComponentPositionsBulkComplete({ successes: [], failures: [] })
            );

            expect(saving.saving).toBe(true);
            expect(complete.saving).toBe(false);
        });

        it('accepts current responses and ignores stale position responses', () => {
            const current = makeProcessor('proc-1', { revision: { version: 5 } });
            const newer = makeProcessor('proc-1', { revision: { version: 6 }, position: { x: 40, y: 50 } });
            const stale = makeProcessor('proc-1', { revision: { version: 4 }, position: { x: 1, y: 2 } });
            const state = createPopulatedState({ processors: [current] });

            const updated = connectorCanvasReducer(
                state,
                ConnectorCanvasActions.updateComponentPositionSuccess({
                    componentId: 'proc-1',
                    componentType: ComponentType.Processor,
                    component: newer
                })
            );
            const unchanged = connectorCanvasReducer(
                updated,
                ConnectorCanvasActions.updateComponentPositionSuccess({
                    componentId: 'proc-1',
                    componentType: ComponentType.Processor,
                    component: stale
                })
            );

            expect(updated.processors[0]).toEqual(newer);
            expect(unchanged.processors[0]).toEqual(newer);
        });

        it('optimistically removes moved components and clears saving on failure', () => {
            const action = ConnectorCanvasActions.moveCanvasComponents({
                processGroupId: 'pg-current',
                targetGroupId: 'pg-child-1',
                components: [
                    {
                        id: 'proc-1',
                        type: ComponentType.Processor,
                        revision: { version: 3, clientId: 'local' }
                    }
                ]
            });

            const moved = connectorCanvasReducer(createPopulatedState(), action);
            const failed = connectorCanvasReducer(
                moved,
                ConnectorCanvasActions.moveCanvasComponentsFailure({
                    components: action.components,
                    error: 'conflict'
                })
            );

            expect(moved.processors.some((processor) => processor.id === 'proc-1')).toBe(false);
            expect(moved.saving).toBe(true);
            expect(failed.saving).toBe(false);
        });

        it('keeps a moved component absent across stale loads until the server confirms the move', () => {
            const component = {
                id: 'proc-1',
                type: ComponentType.Processor,
                revision: { version: 3, clientId: 'local' }
            };
            let state = connectorCanvasReducer(
                createPopulatedState(),
                ConnectorCanvasActions.moveCanvasComponents({
                    processGroupId: 'pg-current',
                    targetGroupId: 'pg-child-1',
                    components: [component]
                })
            );
            const staleLoad = ConnectorCanvasActions.loadConnectorFlowSuccess({
                connectorId: 'connector-1',
                processGroupId: 'pg-current',
                parentProcessGroupId: 'pg-parent',
                breadcrumb: null,
                labels: [],
                funnels: [],
                inputPorts: [],
                outputPorts: [],
                remoteProcessGroups: [],
                processGroups: [],
                processors: [makeProcessor('proc-1')],
                connections: []
            });

            state = connectorCanvasReducer(state, staleLoad);
            state = connectorCanvasReducer(state, staleLoad);
            state = connectorCanvasReducer(
                state,
                ConnectorCanvasActions.moveCanvasComponentsSuccess({
                    targetGroupId: 'pg-child-1',
                    components: [component]
                })
            );

            expect(state.processors.some((processor) => processor.id === 'proc-1')).toBe(false);
            expect(state.removedCache).toContain('proc-1');

            state = connectorCanvasReducer(
                state,
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    ...staleLoad,
                    processors: []
                })
            );
            expect(state.removedCache).not.toContain('proc-1');
        });

        it('restores a moved component after failure releases its tombstone', () => {
            const component = {
                id: 'proc-1',
                type: ComponentType.Processor,
                revision: { version: 3, clientId: 'local' }
            };
            const moved = connectorCanvasReducer(
                createPopulatedState(),
                ConnectorCanvasActions.moveCanvasComponents({
                    processGroupId: 'pg-current',
                    targetGroupId: 'pg-child-1',
                    components: [component]
                })
            );
            const failed = connectorCanvasReducer(
                moved,
                ConnectorCanvasActions.moveCanvasComponentsFailure({
                    components: [component],
                    error: 'conflict'
                })
            );
            const restored = connectorCanvasReducer(
                failed,
                ConnectorCanvasActions.loadConnectorFlowSuccess({
                    connectorId: 'connector-1',
                    processGroupId: 'pg-current',
                    parentProcessGroupId: 'pg-parent',
                    breadcrumb: null,
                    labels: [],
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors: [makeProcessor('proc-1')],
                    connections: []
                })
            );

            expect(restored.processors.some((processor) => processor.id === 'proc-1')).toBe(true);
        });
    });
});
