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

import { reportingTasksReducer, initialState } from './reporting-tasks.reducer';
import { clearReportingTaskBulletinsSuccess } from './reporting-tasks.actions';
import { ReportingTasksState } from './index';
import { BulletinEntity, ComponentType } from '@nifi/shared';

describe('Reporting Tasks Reducer', () => {
    describe('clearReportingTaskBulletinsSuccess', () => {
        let testState: ReportingTasksState;
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
                    level: 'WARN',
                    message: `Test message ${id}`,
                    timestamp: timeOnly,
                    timestampIso: isoString
                }
            };
        };

        beforeEach(() => {
            testState = {
                ...initialState,
                reportingTasks: [
                    {
                        id: 'reporting-task-1',
                        uri: 'test-uri-1',
                        status: { runStatus: 'STOPPED', validationStatus: 'VALID' },
                        bulletins: [
                            createTestBulletin(1, olderTimestamp), // Should be removed
                            createTestBulletin(2, testTimestamp), // Should be kept (equal)
                            createTestBulletin(3, newerTimestamp) // Should be kept
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'reporting-task-1',
                            name: 'Test Reporting Task',
                            type: 'org.apache.nifi.TestReportingTask',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'RUNNING',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            schedulingPeriod: '1 min',
                            schedulingStrategy: 'TIMER_DRIVEN'
                        }
                    },
                    {
                        id: 'reporting-task-2',
                        uri: 'test-uri-2',
                        status: { runStatus: 'STOPPED', validationStatus: 'VALID' },
                        bulletins: [
                            createTestBulletin(4, olderTimestamp) // Should be removed
                        ],
                        revision: { version: 1, clientId: 'test' },
                        permissions: { canRead: true, canWrite: true },
                        component: {
                            id: 'reporting-task-2',
                            name: 'Another Reporting Task',
                            type: 'org.apache.nifi.AnotherReportingTask',
                            bundle: { group: 'org.apache.nifi', artifact: 'test', version: '1.0.0' },
                            state: 'STOPPED',
                            properties: {},
                            descriptors: {},
                            validationErrors: [],
                            schedulingPeriod: '5 min',
                            schedulingStrategy: 'TIMER_DRIVEN'
                        }
                    }
                ]
            };
        });

        it('should replace bulletins for the specified reporting task', () => {
            // Server returns bulletins that remain after clearing (equal and newer bulletins)
            const action = clearReportingTaskBulletinsSuccess({
                response: {
                    componentId: 'reporting-task-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.ReportingTask
                }
            });

            const newState = reportingTasksReducer(testState, action);

            const reportingTask = newState.reportingTasks.find((rt) => rt.id === 'reporting-task-1');
            expect(reportingTask).toBeDefined();
            expect(reportingTask!.bulletins).toHaveLength(2);
            expect(reportingTask!.bulletins!.map((b) => b.id)).toEqual([2, 3]); // Equal and newer bulletins remain
        });

        it('should not affect other reporting tasks', () => {
            const action = clearReportingTaskBulletinsSuccess({
                response: {
                    componentId: 'reporting-task-1',
                    bulletinsCleared: 1,
                    bulletins: [createTestBulletin(2, testTimestamp), createTestBulletin(3, newerTimestamp)],
                    componentType: ComponentType.ReportingTask
                }
            });

            const newState = reportingTasksReducer(testState, action);

            const otherReportingTask = newState.reportingTasks.find((rt) => rt.id === 'reporting-task-2');
            expect(otherReportingTask).toBeDefined();
            expect(otherReportingTask!.bulletins).toHaveLength(1); // Original count unchanged
            expect(otherReportingTask!.bulletins![0].id).toBe(4);
        });

        it('should handle reporting task with no bulletins', () => {
            testState.reportingTasks[0].bulletins = [] as any;

            const action = clearReportingTaskBulletinsSuccess({
                response: {
                    componentId: 'reporting-task-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ReportingTask
                }
            });

            const newState = reportingTasksReducer(testState, action);

            const reportingTask = newState.reportingTasks.find((rt) => rt.id === 'reporting-task-1');
            expect(reportingTask).toBeDefined();
            expect(reportingTask!.bulletins).toEqual([]);
        });

        it('should handle reporting task with empty bulletins array', () => {
            testState.reportingTasks[0].bulletins = [];

            const action = clearReportingTaskBulletinsSuccess({
                response: {
                    componentId: 'reporting-task-1',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ReportingTask
                }
            });

            const newState = reportingTasksReducer(testState, action);

            const reportingTask = newState.reportingTasks.find((rt) => rt.id === 'reporting-task-1');
            expect(reportingTask).toBeDefined();
            expect(reportingTask!.bulletins).toEqual([]);
        });

        it('should handle non-existent reporting task gracefully', () => {
            const action = clearReportingTaskBulletinsSuccess({
                response: {
                    componentId: 'non-existent-task',
                    bulletinsCleared: 0,
                    bulletins: [],
                    componentType: ComponentType.ReportingTask
                }
            });

            const newState = reportingTasksReducer(testState, action);

            // State should remain unchanged
            expect(newState).toEqual(testState);
        });
    });
});
