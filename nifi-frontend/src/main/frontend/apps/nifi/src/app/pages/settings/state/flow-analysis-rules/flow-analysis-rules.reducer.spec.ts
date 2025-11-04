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

import { flowAnalysisRulesReducer, initialState } from './flow-analysis-rules.reducer';
import { clearFlowAnalysisRuleBulletinsSuccess } from './flow-analysis-rules.actions';
import { FlowAnalysisRulesState } from './index';
import { BulletinEntity, ComponentType } from '@nifi/shared';

describe('Flow Analysis Rules Reducer', () => {
    describe('clearFlowAnalysisRuleBulletinsSuccess', () => {
        let testState: FlowAnalysisRulesState;
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
                    level: 'TRACE',
                    message: `Test message ${id}`,
                    timestamp: timeOnly,
                    timestampIso: isoString
                }
            };
        };

        beforeEach(() => {
            testState = {
                ...initialState,
                flowAnalysisRules: [
                    {
                        id: 'flow-analysis-rule-1',
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
                            id: 'flow-analysis-rule-1',
                            name: 'Test Flow Analysis Rule',
                            type: 'org.apache.nifi.TestFlowAnalysisRule',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'ENABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            enforcementPolicy: 'WARN'
                        }
                    },
                    {
                        id: 'flow-analysis-rule-2',
                        uri: 'test-uri-2',
                        status: { runStatus: 'DISABLED', validationStatus: 'VALID' },
                        bulletins: [
                            createTestBulletin(4, olderTimestamp) // Should be removed
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'flow-analysis-rule-2',
                            name: 'Another Flow Analysis Rule',
                            type: 'org.apache.nifi.AnotherFlowAnalysisRule',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'DISABLED',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            enforcementPolicy: 'ENFORCE'
                        }
                    }
                ]
            };
        });

        it('should replace bulletins for the specified flow analysis rule', () => {
            // Server returns bulletins that remain after clearing (equal and newer bulletins)
            const action = clearFlowAnalysisRuleBulletinsSuccess({
                response: {
                    componentId: 'flow-analysis-rule-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.FlowAnalysisRule
                }
            });

            const newState = flowAnalysisRulesReducer(testState, action);

            const flowAnalysisRule = newState.flowAnalysisRules.find((far) => far.id === 'flow-analysis-rule-1');
            expect(flowAnalysisRule).toBeDefined();
            expect(flowAnalysisRule!.bulletins).toHaveLength(2);
            expect(flowAnalysisRule!.bulletins!.map((b) => b.id)).toEqual([2, 3]); // Equal and newer bulletins remain
        });

        it('should not affect other flow analysis rules', () => {
            const action = clearFlowAnalysisRuleBulletinsSuccess({
                response: {
                    componentId: 'flow-analysis-rule-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.FlowAnalysisRule
                }
            });

            const newState = flowAnalysisRulesReducer(testState, action);

            const otherFlowAnalysisRule = newState.flowAnalysisRules.find((far) => far.id === 'flow-analysis-rule-2');
            expect(otherFlowAnalysisRule).toBeDefined();
            expect(otherFlowAnalysisRule!.bulletins).toHaveLength(1); // Original count unchanged
            expect(otherFlowAnalysisRule!.bulletins![0].id).toBe(4);
        });

        it('should handle flow analysis rule with no bulletins', () => {
            testState.flowAnalysisRules[0].bulletins = [] as any;

            const action = clearFlowAnalysisRuleBulletinsSuccess({
                response: {
                    componentId: 'flow-analysis-rule-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.FlowAnalysisRule
                }
            });

            const newState = flowAnalysisRulesReducer(testState, action);

            const flowAnalysisRule = newState.flowAnalysisRules.find((far) => far.id === 'flow-analysis-rule-1');
            expect(flowAnalysisRule).toBeDefined();
            expect(flowAnalysisRule!.bulletins).toEqual([]);
        });

        it('should handle non-existent flow analysis rule gracefully', () => {
            const action = clearFlowAnalysisRuleBulletinsSuccess({
                response: {
                    componentId: 'non-existent-rule',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.FlowAnalysisRule
                }
            });

            const newState = flowAnalysisRulesReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });
    });
});
