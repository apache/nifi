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

import { managementControllerServicesReducer, initialState } from './management-controller-services.reducer';
import { clearControllerServiceBulletinsSuccess } from './management-controller-services.actions';
import { ManagementControllerServicesState } from './index';
import { BulletinEntity, ComponentType } from '@nifi/shared';

describe('Management Controller Services Reducer', () => {
    describe('clearControllerServiceBulletinsSuccess', () => {
        let testState: ManagementControllerServicesState;
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
                controllerServices: [
                    {
                        id: 'controller-service-1',
                        uri: 'test-uri-1',
                        status: { runStatus: 'ENABLED', validationStatus: 'VALID' },
                        bulletins: [
                            createTestBulletin(1, olderTimestamp), // Should be removed
                            createTestBulletin(2, testTimestamp), // Should be kept (equal)
                            createTestBulletin(3, newerTimestamp) // Should be kept
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'controller-service-1',
                            name: 'Test Controller Service',
                            type: 'org.apache.nifi.TestControllerService',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'ENABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: []
                        }
                    },
                    {
                        id: 'controller-service-2',
                        uri: 'test-uri-2',
                        status: { runStatus: 'DISABLED', validationStatus: 'VALID' },
                        bulletins: [
                            createTestBulletin(4, olderTimestamp) // Should be removed
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'controller-service-2',
                            name: 'Another Controller Service',
                            type: 'org.apache.nifi.AnotherControllerService',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'DISABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: []
                        }
                    }
                ]
            };
        });

        it('should replace bulletins for the specified controller service', () => {
            // Server returns bulletins that remain after clearing (equal and newer bulletins)
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = managementControllerServicesReducer(testState, action);

            const controllerService = newState.controllerServices.find((cs) => cs.id === 'controller-service-1');
            expect(controllerService).toBeDefined();
            expect(controllerService!.bulletins).toHaveLength(2);
            expect(controllerService!.bulletins!.map((b) => b.id)).toEqual([2, 3]); // Equal and newer bulletins remain
        });

        it('should not affect other controller services', () => {
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = managementControllerServicesReducer(testState, action);

            const otherControllerService = newState.controllerServices.find((cs) => cs.id === 'controller-service-2');
            expect(otherControllerService).toBeDefined();
            expect(otherControllerService!.bulletins).toHaveLength(1); // Original count unchanged
            expect(otherControllerService!.bulletins![0].id).toBe(4);
        });

        it('should handle controller service with no bulletins', () => {
            testState.controllerServices[0].bulletins = [] as any;

            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = managementControllerServicesReducer(testState, action);

            const controllerService = newState.controllerServices.find((cs) => cs.id === 'controller-service-1');
            expect(controllerService).toBeDefined();
            expect(controllerService!.bulletins).toEqual([]);
        });

        it('should handle controller service with empty bulletins array', () => {
            testState.controllerServices[0].bulletins = [];

            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = managementControllerServicesReducer(testState, action);

            const controllerService = newState.controllerServices.find((cs) => cs.id === 'controller-service-1');
            expect(controllerService).toBeDefined();
            expect(controllerService!.bulletins).toEqual([]);
        });

        it('should handle non-existent controller service gracefully', () => {
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'non-existent-service',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = managementControllerServicesReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });

        it('should remove all bulletins older than the specified timestamp', () => {
            testState.controllerServices[1].bulletins = [
                createTestBulletin(5, olderTimestamp),
                createTestBulletin(6, new Date('2023-01-01T10:00:00.000Z').getTime()), // Even older
                createTestBulletin(7, newerTimestamp)
            ];

            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-2',
                    bulletinsCleared: 2,
                    bulletins: [createTestBulletin(7, newerTimestamp)], // Only newer bulletin remains
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = managementControllerServicesReducer(testState, action);

            const controllerService = newState.controllerServices.find((cs) => cs.id === 'controller-service-2');
            expect(controllerService).toBeDefined();
            expect(controllerService!.bulletins).toHaveLength(1);
            expect(controllerService!.bulletins![0].id).toBe(7); // Only newer bulletin remains
        });
    });
});
