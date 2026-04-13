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
            labels: [{ id: 'label-1' }],
            funnels: [{ id: 'funnel-1' }],
            inputPorts: [{ id: 'input-1' }],
            outputPorts: [{ id: 'output-1' }],
            remoteProcessGroups: [{ id: 'rpg-1' }],
            processGroups: [{ id: 'pg-child-1' }],
            processors: [{ id: 'proc-1' }, { id: 'proc-2' }],
            connections: [{ id: 'conn-1' }],
            registryClients: [],
            skipTransform: false,
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
    });

    describe('loadConnectorFlowSuccess', () => {
        it('should populate state with flow data', () => {
            const processors = [{ id: 'proc-new' }];
            const connections = [{ id: 'conn-new' }];
            const labels = [{ id: 'label-new' }];

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
    });

    describe('loadConnectorFlowFailure', () => {
        it('should reset to initial state with error', () => {
            const previousState = createPopulatedState();

            const result = connectorCanvasReducer(
                previousState,
                ConnectorCanvasActions.loadConnectorFlowFailure({
                    errorContext: {
                        context: ErrorContextKey.CONNECTORS,
                        errors: ['Connection refused']
                    }
                })
            );

            expect(result.loadingStatus).toBe('error');
            expect(result.error).toBe('Connection refused');
            expect(result.processors).toEqual([]);
            expect(result.connections).toEqual([]);
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
});
