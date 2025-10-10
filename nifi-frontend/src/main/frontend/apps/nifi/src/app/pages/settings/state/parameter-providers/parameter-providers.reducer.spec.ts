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

import { parameterProvidersReducer, initialParameterProvidersState } from './parameter-providers.reducer';
import { clearParameterProviderBulletinsSuccess } from './parameter-providers.actions';
import { ParameterProvidersState } from './index';
import { BulletinEntity, ComponentType } from '@nifi/shared';

describe('Parameter Providers Reducer', () => {
    describe('clearParameterProviderBulletinsSuccess', () => {
        let testState: ParameterProvidersState;
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
                    level: 'INFO',
                    message: `Test message ${id}`,
                    timestamp: timeOnly,
                    timestampIso: isoString
                }
            };
        };

        beforeEach(() => {
            testState = {
                ...initialParameterProvidersState,
                parameterProviders: [
                    {
                        id: 'parameter-provider-1',
                        uri: 'test-uri-1',
                        bulletins: [
                            createTestBulletin(1, olderTimestamp), // Should be removed
                            createTestBulletin(2, testTimestamp), // Should be kept (equal)
                            createTestBulletin(3, newerTimestamp) // Should be kept
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'parameter-provider-1',
                            name: 'Test Parameter Provider',
                            type: 'org.apache.nifi.TestParameterProvider',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            affectedComponents: [],
                            parameterGroupConfigurations: [],
                            comments: '',
                            deprecated: false,
                            extensionMissing: false,
                            multipleVersionsAvailable: false,
                            persistsState: false,
                            referencingParameterContexts: [],
                            restricted: false,
                            validationStatus: 'VALID'
                        }
                    },
                    {
                        id: 'parameter-provider-2',
                        uri: 'test-uri-2',
                        bulletins: [
                            createTestBulletin(4, olderTimestamp) // Should be removed
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'parameter-provider-2',
                            name: 'Another Parameter Provider',
                            type: 'org.apache.nifi.AnotherParameterProvider',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            affectedComponents: [],
                            parameterGroupConfigurations: [],
                            comments: '',
                            deprecated: false,
                            extensionMissing: false,
                            multipleVersionsAvailable: false,
                            persistsState: false,
                            referencingParameterContexts: [],
                            restricted: false,
                            validationStatus: 'VALID'
                        }
                    }
                ]
            };
        });

        it('should replace bulletins for the specified parameter provider', () => {
            // Server returns bulletins that remain after clearing (equal and newer bulletins)
            const action = clearParameterProviderBulletinsSuccess({
                response: {
                    componentId: 'parameter-provider-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.ParameterProvider
                }
            });

            const newState = parameterProvidersReducer(testState, action);

            const parameterProvider = newState.parameterProviders.find((pp) => pp.id === 'parameter-provider-1');
            expect(parameterProvider).toBeDefined();
            expect(parameterProvider!.bulletins).toHaveLength(2);
            expect(parameterProvider!.bulletins!.map((b) => b.id)).toEqual([2, 3]); // Equal and newer bulletins remain
        });

        it('should not affect other parameter providers', () => {
            const action = clearParameterProviderBulletinsSuccess({
                response: {
                    componentId: 'parameter-provider-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.ParameterProvider
                }
            });

            const newState = parameterProvidersReducer(testState, action);

            const otherParameterProvider = newState.parameterProviders.find((pp) => pp.id === 'parameter-provider-2');
            expect(otherParameterProvider).toBeDefined();
            expect(otherParameterProvider!.bulletins).toHaveLength(1); // Original count unchanged
            expect(otherParameterProvider!.bulletins![0].id).toBe(4);
        });

        it('should handle parameter provider with no bulletins', () => {
            testState.parameterProviders[0].bulletins = [] as any;

            const action = clearParameterProviderBulletinsSuccess({
                response: {
                    componentId: 'parameter-provider-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ParameterProvider
                }
            });

            const newState = parameterProvidersReducer(testState, action);

            const parameterProvider = newState.parameterProviders.find((pp) => pp.id === 'parameter-provider-1');
            expect(parameterProvider).toBeDefined();
            expect(parameterProvider!.bulletins).toEqual([]);
        });

        it('should handle non-existent parameter provider gracefully', () => {
            const action = clearParameterProviderBulletinsSuccess({
                response: {
                    componentId: 'non-existent-provider',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ParameterProvider
                }
            });

            const newState = parameterProvidersReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });
    });
});
