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

import { controllerServicesReducer, initialState } from './controller-services.reducer';
import { clearControllerServiceBulletinsSuccess } from './controller-services.actions';
import { ControllerServicesState } from './index';
import { ComponentType, BulletinEntity } from '@nifi/shared';

describe('Controller Services Reducer', () => {
    describe('clearControllerServiceBulletinsSuccess', () => {
        let testState: ControllerServicesState;
        const testTimestamp = new Date('2023-01-01T12:00:00.000Z').getTime();
        const olderTimestamp = new Date('2023-01-01T11:00:00.000Z').getTime();
        const newerTimestamp = new Date('2023-01-01T13:00:00.000Z').getTime();

        const createTestBulletin = (id: number, timestamp: number): BulletinEntity => {
            const isoString = new Date(timestamp).toISOString();
            const timeOnly = isoString.substring(11, 19) + ' UTC'; // Extract HH:MM:SS and add UTC
            return {
                id,
                canRead: true,
                sourceId: 'controller-service-1',
                groupId: 'group1',
                timestamp: timeOnly,
                timestampIso: isoString,
                bulletin: {
                    id,
                    nodeAddress: 'localhost',
                    category: 'test',
                    groupId: 'group1',
                    sourceId: 'controller-service-1',
                    sourceName: 'Test Controller Service',
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
                        bulletins: [
                            createTestBulletin(1, olderTimestamp), // Should be removed
                            createTestBulletin(2, newerTimestamp) // Should be kept
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'controller-service-1',
                            name: 'Test Controller Service 1',
                            type: 'org.apache.nifi.TestControllerService',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'ENABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            referencingComponents: [],
                            comments: '',
                            deprecated: false,
                            extensionMissing: false,
                            multipleVersionsAvailable: false,
                            persistsState: false,
                            restricted: false,
                            validationStatus: 'VALID'
                        },
                        status: {
                            runStatus: 'ENABLED',
                            validationStatus: 'VALID'
                        }
                    } as any,
                    {
                        id: 'controller-service-2',
                        uri: 'test-uri-2',
                        bulletins: [
                            createTestBulletin(3, olderTimestamp), // Should be removed
                            createTestBulletin(4, testTimestamp), // Should be kept (equal to timestamp)
                            createTestBulletin(5, newerTimestamp) // Should be kept
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'controller-service-2',
                            name: 'Test Controller Service 2',
                            type: 'org.apache.nifi.TestControllerService',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'ENABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            referencingComponents: [],
                            comments: '',
                            deprecated: false,
                            extensionMissing: false,
                            multipleVersionsAvailable: false,
                            persistsState: false,
                            restricted: false,
                            validationStatus: 'VALID'
                        },
                        status: {
                            runStatus: 'ENABLED',
                            validationStatus: 'VALID'
                        }
                    } as any,
                    {
                        id: 'controller-service-3',
                        uri: 'test-uri-3',
                        bulletins: [
                            createTestBulletin(6, olderTimestamp) // Should be removed
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'controller-service-3',
                            name: 'Test Controller Service 3',
                            type: 'org.apache.nifi.TestControllerService',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'ENABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            referencingComponents: [],
                            comments: '',
                            deprecated: false,
                            extensionMissing: false,
                            multipleVersionsAvailable: false,
                            persistsState: false,
                            restricted: false,
                            validationStatus: 'VALID'
                        },
                        status: {
                            runStatus: 'ENABLED',
                            validationStatus: 'VALID'
                        }
                    } as any
                ]
            };
        });

        it('should replace bulletins for controller service', () => {
            // Server returns bulletins that remain after clearing (newer bulletins)
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            const service = newState.controllerServices.find((s) => s.id === 'controller-service-1') as any;
            expect(service.bulletins).toHaveLength(1);
            expect(service.bulletins[0].id).toBe(2); // Only newer bulletin remains
        });

        it('should replace bulletins keeping equal and newer timestamps', () => {
            // Server returns bulletins that remain after clearing (equal and newer bulletins)
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-2',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(4, testTimestamp), createTestBulletin(5, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            const service = newState.controllerServices.find((s) => s.id === 'controller-service-2') as any;
            expect(service.bulletins).toHaveLength(2);
            expect(service.bulletins.map((b: any) => b.id)).toEqual([4, 5]); // Equal and newer bulletins remain
        });

        it('should remove all older bulletins when all are older than timestamp', () => {
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-3',
                    bulletinsCleared: 1,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            const service = newState.controllerServices.find((s) => s.id === 'controller-service-3') as any;
            expect(service.bulletins).toHaveLength(0); // All bulletins removed
        });

        it('should not affect other controller services when updating specific service', () => {
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            // Other services should remain unchanged
            const service2 = newState.controllerServices.find((s) => s.id === 'controller-service-2') as any;
            expect(service2.bulletins).toHaveLength(3); // Original count unchanged

            const service3 = newState.controllerServices.find((s) => s.id === 'controller-service-3') as any;
            expect(service3.bulletins).toHaveLength(1); // Original count unchanged
        });

        it('should handle controller service with no bulletins', () => {
            testState.controllerServices.push({
                id: 'controller-service-no-bulletins',
                uri: 'test-uri-no-bulletins',
                bulletins: undefined,
                revision: { version: 1, clientId: 'test' },
                permissions: { canRead: true, canWrite: true },
                component: {
                    id: 'controller-service-no-bulletins',
                    name: 'Test Controller Service No Bulletins',
                    type: 'org.apache.nifi.TestControllerService',
                    bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                    state: 'ENABLED',
                    properties: {},
                    descriptors: {},
                    validationErrors: [],
                    referencingComponents: [],
                    comments: '',
                    deprecated: false,
                    extensionMissing: false,
                    multipleVersionsAvailable: false,
                    persistsState: false,
                    restricted: false,
                    validationStatus: 'VALID'
                },
                status: {
                    runStatus: 'ENABLED',
                    validationStatus: 'VALID'
                }
            } as any);

            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-no-bulletins',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            const service = newState.controllerServices.find((s) => s.id === 'controller-service-no-bulletins') as any;
            expect(service).toBeDefined();
            expect(service.bulletins).toEqual([]);
        });

        it('should handle controller service with empty bulletins array', () => {
            testState.controllerServices.push({
                id: 'controller-service-empty-bulletins',
                uri: 'test-uri-empty-bulletins',
                bulletins: [],
                revision: { version: 1, clientId: 'test' },
                permissions: { canRead: true, canWrite: true },
                component: {
                    id: 'controller-service-empty-bulletins',
                    name: 'Test Controller Service Empty Bulletins',
                    type: 'org.apache.nifi.TestControllerService',
                    bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                    state: 'ENABLED',
                    properties: {},
                    descriptors: {},
                    validationErrors: [],
                    referencingComponents: [],
                    comments: '',
                    deprecated: false,
                    extensionMissing: false,
                    multipleVersionsAvailable: false,
                    persistsState: false,
                    restricted: false,
                    validationStatus: 'VALID'
                },
                status: {
                    runStatus: 'ENABLED',
                    validationStatus: 'VALID'
                }
            } as any);

            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-empty-bulletins',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            const service = newState.controllerServices.find(
                (s) => s.id === 'controller-service-empty-bulletins'
            ) as any;
            expect(service).toBeDefined();
            expect(service.bulletins).toEqual([]);
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

            const newState = controllerServicesReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });

        it('should handle empty controller services array gracefully', () => {
            const emptyState: ControllerServicesState = {
                ...initialState,
                controllerServices: []
            };

            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'any-service',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(emptyState, action);

            // State should remain unchanged
            expect(newState).toEqual(emptyState);
        });

        it('should preserve all other controller service properties when filtering bulletins', () => {
            const originalService = testState.controllerServices[0];
            const action = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const newState = controllerServicesReducer(testState, action);

            const updatedService = newState.controllerServices.find((s) => s.id === 'controller-service-1');
            expect(updatedService).toBeDefined();

            // All properties except bulletins should remain the same
            expect(updatedService!.id).toBe(originalService.id);
            expect(updatedService!.uri).toBe(originalService.uri);
            expect(updatedService!.revision).toEqual(originalService.revision);
            expect(updatedService!.permissions).toEqual(originalService.permissions);
            expect(updatedService!.component).toEqual(originalService.component);
            expect(updatedService!.status).toEqual(originalService.status);

            // Only bulletins should be filtered
            expect((updatedService as any).bulletins).toHaveLength(1);
            expect((updatedService as any).bulletins[0].id).toBe(2);
        });

        it('should handle multiple bulletin clearing operations correctly', () => {
            // First operation - clear service 1
            const action1 = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const intermediateState = controllerServicesReducer(testState, action1);

            // Second operation - clear service 2
            const action2 = clearControllerServiceBulletinsSuccess({
                response: {
                    componentId: 'controller-service-2',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(4, testTimestamp), createTestBulletin(5, newerTimestamp)],
                    componentType: ComponentType.ControllerService
                }
            });

            const finalState = controllerServicesReducer(intermediateState, action2);

            // Both services should be updated correctly
            const service1 = finalState.controllerServices.find((s) => s.id === 'controller-service-1') as any;
            expect(service1.bulletins).toHaveLength(1);
            expect(service1.bulletins[0].id).toBe(2);

            const service2 = finalState.controllerServices.find((s) => s.id === 'controller-service-2') as any;
            expect(service2.bulletins).toHaveLength(2);
            expect(service2.bulletins.map((b: any) => b.id)).toEqual([4, 5]);

            // Service 3 should remain unchanged
            const service3 = finalState.controllerServices.find((s) => s.id === 'controller-service-3') as any;
            expect(service3.bulletins).toHaveLength(1);
            expect(service3.bulletins[0].id).toBe(6);
        });
    });
});
