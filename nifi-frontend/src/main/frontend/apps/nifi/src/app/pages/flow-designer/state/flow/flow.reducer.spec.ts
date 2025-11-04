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

import { flowReducer, initialState } from './flow.reducer';
import { clearBulletinsForComponentSuccess } from './flow.actions';
import { FlowState } from './index';
import { BulletinEntity, ComponentType } from '@nifi/shared';

describe('Flow Reducer', () => {
    describe('clearBulletinsForComponentSuccess', () => {
        let testState: FlowState;
        const testTimestamp = new Date('2023-01-01T12:00:00.000Z').getTime();
        const olderTimestamp = new Date('2023-01-01T11:00:00.000Z').getTime();
        const newerTimestamp = new Date('2023-01-01T13:00:00.000Z').getTime();

        const createTestBulletin = (id: number, timestamp: number): BulletinEntity => {
            const isoString = new Date(timestamp).toISOString();
            const timeOnly = isoString.substring(11, 19) + ' UTC'; // Extract HH:MM:SS and add UTC
            return {
                id,
                canRead: true,
                sourceId: 'source1',
                groupId: 'group1',
                timestamp: timeOnly,
                timestampIso: isoString,
                bulletin: {
                    id,
                    nodeAddress: 'localhost',
                    category: 'test',
                    groupId: 'group1',
                    sourceId: 'source1',
                    sourceName: 'Test Source',
                    sourceType: 'COMPONENT',
                    level: 'ERROR',
                    message: `Test message ${id}`,
                    timestamp: timeOnly,
                    timestampIso: isoString
                }
            };
        };

        beforeEach(() => {
            testState = {
                ...initialState,
                flow: {
                    ...initialState.flow,
                    processGroupFlow: {
                        ...initialState.flow.processGroupFlow,
                        flow: {
                            ...initialState.flow.processGroupFlow.flow,
                            processors: [
                                {
                                    id: 'processor-1',
                                    bulletins: [
                                        createTestBulletin(1, olderTimestamp), // Should be removed
                                        createTestBulletin(2, newerTimestamp) // Should be kept
                                    ],
                                    revision: { version: 1, clientId: 'test' },
                                    permissions: { canRead: true, canWrite: true },
                                    position: { x: 0, y: 0 },
                                    component: { id: 'processor-1', type: 'Processor' }
                                } as any
                            ],
                            inputPorts: [
                                {
                                    id: 'input-port-1',
                                    bulletins: [
                                        createTestBulletin(3, olderTimestamp), // Should be removed
                                        createTestBulletin(4, testTimestamp), // Should be kept (equal to timestamp)
                                        createTestBulletin(5, newerTimestamp) // Should be kept
                                    ],
                                    revision: { version: 1, clientId: 'test' },
                                    permissions: { canRead: true, canWrite: true },
                                    position: { x: 0, y: 0 },
                                    component: { id: 'input-port-1', type: 'InputPort' }
                                } as any
                            ],
                            outputPorts: [
                                {
                                    id: 'output-port-1',
                                    bulletins: [
                                        createTestBulletin(6, olderTimestamp) // Should be removed
                                    ],
                                    revision: { version: 1, clientId: 'test' },
                                    permissions: { canRead: true, canWrite: true },
                                    position: { x: 0, y: 0 },
                                    component: { id: 'output-port-1', type: 'OutputPort' }
                                } as any
                            ],
                            remoteProcessGroups: [
                                {
                                    id: 'remote-pg-1',
                                    bulletins: [
                                        createTestBulletin(7, newerTimestamp) // Should be kept
                                    ],
                                    revision: { version: 1, clientId: 'test' },
                                    permissions: { canRead: true, canWrite: true },
                                    position: { x: 0, y: 0 },
                                    component: { id: 'remote-pg-1', type: 'RemoteProcessGroup' }
                                } as any
                            ]
                        }
                    }
                }
            };
        });

        it('should replace bulletins for processors', () => {
            // Provide the bulletins that should remain after clearing
            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'processor-1',
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    bulletinsCleared: 1,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(testState, action);

            const processor = newState.flow.processGroupFlow.flow.processors![0] as any;
            expect(processor.bulletins).toHaveLength(1);
            expect(processor.bulletins![0].id).toBe(2); // Only newer bulletin remains
        });

        it('should replace bulletins for input ports', () => {
            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'input-port-1',
                    bulletins: [createTestBulletin(4, testTimestamp), createTestBulletin(5, newerTimestamp)],
                    bulletinsCleared: 1,
                    componentType: ComponentType.InputPort
                }
            });

            const newState = flowReducer(testState, action);

            const inputPort = newState.flow.processGroupFlow.flow.inputPorts![0] as any;
            expect(inputPort.bulletins).toHaveLength(2);
            expect(inputPort.bulletins!.map((b: any) => b.id)).toEqual([4, 5]); // Equal and newer bulletins remain
        });

        it('should filter older bulletins from output ports', () => {
            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'output-port-1',
                    bulletins: [],
                    bulletinsCleared: 1,
                    componentType: ComponentType.OutputPort
                }
            });

            const newState = flowReducer(testState, action);

            const outputPort = newState.flow.processGroupFlow.flow.outputPorts![0] as any;
            expect(outputPort.bulletins).toHaveLength(0); // All bulletins removed
        });

        it('should replace bulletins for remote process groups', () => {
            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'remote-pg-1',
                    bulletins: [createTestBulletin(7, newerTimestamp)],
                    bulletinsCleared: 0,
                    componentType: ComponentType.RemoteProcessGroup
                }
            });

            const newState = flowReducer(testState, action);

            const remotePg = newState.flow.processGroupFlow.flow.remoteProcessGroups![0] as any;
            expect(remotePg.bulletins).toHaveLength(1);
            expect(remotePg.bulletins![0].id).toBe(7); // Newer bulletin remains
        });

        it('should not affect other components when updating specific component', () => {
            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'processor-1',
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    bulletinsCleared: 1,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(testState, action);

            // Other components should remain unchanged
            const inputPort = newState.flow.processGroupFlow.flow.inputPorts![0] as any;
            expect(inputPort.bulletins).toHaveLength(3); // Original count unchanged

            const outputPort = newState.flow.processGroupFlow.flow.outputPorts![0] as any;
            expect(outputPort.bulletins).toHaveLength(1); // Original count unchanged

            const remotePg = newState.flow.processGroupFlow.flow.remoteProcessGroups![0] as any;
            expect(remotePg.bulletins).toHaveLength(1); // Original count unchanged
        });

        it('should handle component with no bulletins', () => {
            // Add a component with no bulletins
            testState.flow.processGroupFlow.flow.processors!.push({
                id: 'processor-no-bulletins',
                bulletins: undefined,
                revision: { version: 1, clientId: 'test' },
                permissions: { canRead: true, canWrite: true },
                position: { x: 0, y: 0 },
                component: { id: 'processor-no-bulletins', type: 'Processor' }
            } as any);

            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'processor-no-bulletins',
                    bulletins: [],
                    bulletinsCleared: 0,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(testState, action);

            const processor = newState.flow.processGroupFlow.flow.processors!.find(
                (p) => p.id === 'processor-no-bulletins'
            ) as any;
            expect(processor).toBeDefined();
            expect(processor!.bulletins).toEqual([]);
        });

        it('should handle component with empty bulletins array', () => {
            // Add a component with empty bulletins array
            testState.flow.processGroupFlow.flow.processors!.push({
                id: 'processor-empty-bulletins',
                bulletins: [],
                revision: { version: 1, clientId: 'test' },
                permissions: { canRead: true, canWrite: true },
                position: { x: 0, y: 0 },
                component: { id: 'processor-empty-bulletins', type: 'Processor' }
            } as any);

            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'processor-empty-bulletins',
                    bulletins: [],
                    bulletinsCleared: 0,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(testState, action);

            const processor = newState.flow.processGroupFlow.flow.processors!.find(
                (p) => p.id === 'processor-empty-bulletins'
            ) as any;
            expect(processor).toBeDefined();
            expect(processor!.bulletins).toEqual([]);
        });

        it('should handle non-existent component gracefully', () => {
            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'non-existent-component',
                    bulletins: [],
                    bulletinsCleared: 0,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });

        it('should handle missing collections gracefully', () => {
            // Create state with missing collections
            const stateWithMissingCollections: FlowState = {
                ...initialState,
                flow: {
                    ...initialState.flow,
                    processGroupFlow: {
                        ...initialState.flow.processGroupFlow,
                        flow: {
                            ...initialState.flow.processGroupFlow.flow,
                            processors: null as any,
                            inputPorts: null as any,
                            outputPorts: null as any,
                            remoteProcessGroups: null as any
                        }
                    }
                }
            };

            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'any-component',
                    bulletins: [],
                    bulletinsCleared: 0,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(stateWithMissingCollections, action);

            // State should remain unchanged
            expect(newState).toEqual(stateWithMissingCollections);
        });

        it('should break after finding and updating the first matching component', () => {
            // Add duplicate component IDs across different collections to test break logic
            testState.flow.processGroupFlow.flow.inputPorts!.push({
                id: 'processor-1', // Same ID as processor
                bulletins: [createTestBulletin(99, olderTimestamp)],
                revision: { version: 1, clientId: 'test' },
                permissions: { canRead: true, canWrite: true },
                position: { x: 0, y: 0 },
                component: { id: 'processor-1', type: 'InputPort' }
            } as any);

            const action = clearBulletinsForComponentSuccess({
                response: {
                    componentId: 'processor-1',
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    bulletinsCleared: 1,
                    componentType: ComponentType.Processor
                }
            });

            const newState = flowReducer(testState, action);

            // Only processor should be updated (first match), input port should remain unchanged
            const processor = newState.flow.processGroupFlow.flow.processors![0] as any;
            expect(processor.bulletins).toHaveLength(1);
            expect(processor.bulletins![0].id).toBe(2);

            const inputPortWithSameId = newState.flow.processGroupFlow.flow.inputPorts!.find(
                (p) => p.id === 'processor-1'
            ) as any;
            expect(inputPortWithSameId!.bulletins).toHaveLength(1);
            expect(inputPortWithSameId!.bulletins![0].id).toBe(99); // Unchanged
        });
    });
});
