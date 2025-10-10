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

import { registryClientsReducer, initialState } from './registry-clients.reducer';
import { clearRegistryClientBulletinsSuccess } from './registry-clients.actions';
import { RegistryClientsState } from './index';
import { BulletinEntity, ComponentType } from '@nifi/shared';

describe('Registry Clients Reducer', () => {
    describe('clearRegistryClientBulletinsSuccess', () => {
        let testState: RegistryClientsState;
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
                    level: 'DEBUG',
                    message: `Test message ${id}`,
                    timestamp: timeOnly,
                    timestampIso: isoString
                }
            };
        };

        beforeEach(() => {
            testState = {
                ...initialState,
                registryClients: [
                    {
                        id: 'registry-client-1',
                        uri: 'test-uri-1',
                        bulletins: [
                            createTestBulletin(1, olderTimestamp), // Should be removed
                            createTestBulletin(2, testTimestamp), // Should be kept (equal)
                            createTestBulletin(3, newerTimestamp) // Should be kept
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'registry-client-1',
                            name: 'Test Registry Client',
                            type: 'org.apache.nifi.TestRegistryClient',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            properties: {},
                            descriptors: {},
                            validationErrors: []
                        }
                    },
                    {
                        id: 'registry-client-2',
                        uri: 'test-uri-2',
                        bulletins: [
                            createTestBulletin(4, olderTimestamp) // Should be removed
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'registry-client-2',
                            name: 'Another Registry Client',
                            type: 'org.apache.nifi.AnotherRegistryClient',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            properties: {},
                            descriptors: {},
                            validationErrors: []
                        }
                    }
                ]
            };
        });

        it('should replace bulletins for the specified registry client', () => {
            // Server returns bulletins that remain after clearing (equal and newer bulletins)
            const action = clearRegistryClientBulletinsSuccess({
                response: {
                    componentId: 'registry-client-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.FlowRegistryClient
                }
            });

            const newState = registryClientsReducer(testState, action);

            const registryClient = newState.registryClients.find((rc) => rc.id === 'registry-client-1');
            expect(registryClient).toBeDefined();
            expect(registryClient!.bulletins).toHaveLength(2);
            expect(registryClient!.bulletins!.map((b) => b.id)).toEqual([2, 3]); // Equal and newer bulletins remain
        });

        it('should not affect other registry clients', () => {
            const action = clearRegistryClientBulletinsSuccess({
                response: {
                    componentId: 'registry-client-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.FlowRegistryClient
                }
            });

            const newState = registryClientsReducer(testState, action);

            const otherRegistryClient = newState.registryClients.find((rc) => rc.id === 'registry-client-2');
            expect(otherRegistryClient).toBeDefined();
            expect(otherRegistryClient!.bulletins).toHaveLength(1); // Original count unchanged
            expect(otherRegistryClient!.bulletins![0].id).toBe(4);
        });

        it('should handle registry client with no bulletins', () => {
            testState.registryClients[0].bulletins = undefined;

            const action = clearRegistryClientBulletinsSuccess({
                response: {
                    componentId: 'registry-client-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.FlowRegistryClient
                }
            });

            const newState = registryClientsReducer(testState, action);

            const registryClient = newState.registryClients.find((rc) => rc.id === 'registry-client-1');
            expect(registryClient).toBeDefined();
            expect(registryClient!.bulletins).toEqual([]);
        });

        it('should handle non-existent registry client gracefully', () => {
            const action = clearRegistryClientBulletinsSuccess({
                response: {
                    componentId: 'non-existent-client',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.FlowRegistryClient
                }
            });

            const newState = registryClientsReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });
    });
});
