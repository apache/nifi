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

import { TestBed } from '@angular/core/testing';
import { ReportingTasks } from './reporting-tasks.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/reporting-tasks/reporting-tasks.reducer';
import { ReportingTaskEntity, reportingTasksFeatureKey } from '../../state/reporting-tasks';
import { settingsFeatureKey } from '../../state';
import { ComponentType } from '@nifi/shared';
import * as ReportingTasksActions from '../../state/reporting-tasks/reporting-tasks.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { currentUserFeatureKey } from '../../../../state/current-user';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';
import { flowConfigurationFeatureKey } from '../../../../state/flow-configuration';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';

describe('ReportingTasks', () => {
    // Mock data factories
    function createMockReportingTaskEntity(): ReportingTaskEntity {
        return {
            id: 'test-reporting-task-id',
            uri: 'test-uri',
            bulletins: [
                {
                    id: 1,
                    sourceId: 'test-reporting-task-id',
                    groupId: 'test-group-id',
                    timestamp: '12:00:00 UTC',
                    timestampIso: '2023-10-08T12:00:00.000Z',
                    canRead: true,
                    bulletin: {
                        id: 1,
                        category: 'INFO',
                        level: 'INFO',
                        message: 'Test bulletin',
                        timestamp: '12:00:00 UTC',
                        timestampIso: '2023-10-08T12:00:00.000Z',
                        sourceId: 'test-reporting-task-id',
                        groupId: 'test-group-id',
                        sourceType: 'REPORTING_TASK',
                        sourceName: 'Test Reporting Task'
                    }
                }
            ],
            permissions: {
                canRead: true,
                canWrite: true
            },
            operatePermissions: {
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 1,
                clientId: 'test-client'
            },
            component: {
                id: 'test-reporting-task-id',
                name: 'Test Reporting Task',
                type: 'org.apache.nifi.TestReportingTask',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                state: 'STOPPED',
                comments: 'Test comments',
                persistsState: true,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                schedulingPeriod: '1 min',
                schedulingStrategy: 'TIMER_DRIVEN',
                defaultSchedulingPeriod: {},
                properties: {},
                descriptors: {},
                customUiUrl: '',
                annotationData: '',
                validationErrors: [],
                validationStatus: 'VALID',
                activeThreadCount: 0
            },
            status: {
                runStatus: 'STOPPED',
                validationStatus: 'VALID',
                activeThreadCount: 0
            }
        };
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [ReportingTasks],
            providers: [
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [reportingTasksFeatureKey]: {
                                ...initialState
                            }
                        },
                        [currentUserFeatureKey]: fromCurrentUser.initialState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState
                    }
                })
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(ReportingTasks);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);

        fixture.detectChanges();

        return { component, fixture, store };
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    describe('Component Initialization', () => {
        it('should dispatch loadReportingTasks on ngOnInit', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(store.dispatch).toHaveBeenCalledWith(ReportingTasksActions.loadReportingTasks());
        });

        it('should dispatch resetReportingTasksState on ngOnDestroy', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnDestroy();

            expect(store.dispatch).toHaveBeenCalledWith(ReportingTasksActions.resetReportingTasksState());
        });

        it('should return true for isInitialLoading when timestamp matches initial state', async () => {
            const { component } = await setup();
            const state = { ...initialState };

            expect(component.isInitialLoading(state)).toBe(true);
        });

        it('should return false for isInitialLoading when timestamp differs from initial state', async () => {
            const { component } = await setup();
            const state = {
                ...initialState,
                loadedTimestamp: '2023-01-01 12:00:00 EST'
            };

            expect(component.isInitialLoading(state)).toBe(false);
        });
    });

    describe('Action dispatching', () => {
        it('should dispatch loadReportingTasks action when refreshReportingTaskListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.refreshReportingTaskListing();

            expect(store.dispatch).toHaveBeenCalledWith(ReportingTasksActions.loadReportingTasks());
        });

        it('should dispatch openNewReportingTaskDialog action when openNewReportingTaskDialog is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.openNewReportingTaskDialog();

            expect(store.dispatch).toHaveBeenCalledWith(ReportingTasksActions.openNewReportingTaskDialog());
        });

        it('should dispatch selectReportingTask action when selectReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.selectReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.selectReportingTask({
                    request: {
                        id: mockReportingTaskEntity.id
                    }
                })
            );
        });

        it('should dispatch navigateToAdvancedReportingTaskUi action when openAdvancedUi is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.openAdvancedUi(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.navigateToAdvancedReportingTaskUi({
                    id: mockReportingTaskEntity.id
                })
            );
        });

        it('should dispatch navigateToManageAccessPolicies action when navigateToManageAccessPolicies is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.navigateToManageAccessPolicies(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.navigateToManageAccessPolicies({
                    id: mockReportingTaskEntity.id
                })
            );
        });

        it('should dispatch navigateToComponentDocumentation action when viewReportingTaskDocumentation is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.viewReportingTaskDocumentation(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                navigateToComponentDocumentation({
                    request: {
                        backNavigation: {
                            route: ['/settings', 'reporting-tasks', mockReportingTaskEntity.id],
                            routeBoundary: ['/documentation'],
                            context: 'Reporting Task'
                        },
                        parameters: {
                            componentType: ComponentType.ReportingTask,
                            type: mockReportingTaskEntity.component.type,
                            group: mockReportingTaskEntity.component.bundle.group,
                            artifact: mockReportingTaskEntity.component.bundle.artifact,
                            version: mockReportingTaskEntity.component.bundle.version
                        }
                    }
                })
            );
        });

        it('should dispatch promptReportingTaskDeletion action when deleteReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.deleteReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.promptReportingTaskDeletion({
                    request: {
                        reportingTask: mockReportingTaskEntity
                    }
                })
            );
        });

        it('should dispatch navigateToEditReportingTask action when configureReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.configureReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.navigateToEditReportingTask({
                    id: mockReportingTaskEntity.id
                })
            );
        });

        it('should dispatch getComponentStateAndOpenDialog action when viewStateReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            mockReportingTaskEntity.status.runStatus = 'STOPPED';
            mockReportingTaskEntity.status.activeThreadCount = 0;
            jest.spyOn(store, 'dispatch');

            component.viewStateReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                getComponentStateAndOpenDialog({
                    request: {
                        componentType: ComponentType.ReportingTask,
                        componentId: mockReportingTaskEntity.id,
                        componentName: mockReportingTaskEntity.component.name,
                        canClear: true
                    }
                })
            );
        });

        it('should dispatch getComponentStateAndOpenDialog action with canClear false when task is running', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            mockReportingTaskEntity.status.runStatus = 'RUNNING';
            mockReportingTaskEntity.status.activeThreadCount = 1;
            jest.spyOn(store, 'dispatch');

            component.viewStateReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                getComponentStateAndOpenDialog({
                    request: {
                        componentType: ComponentType.ReportingTask,
                        componentId: mockReportingTaskEntity.id,
                        componentName: mockReportingTaskEntity.component.name,
                        canClear: false
                    }
                })
            );
        });

        it('should dispatch startReportingTask action when startReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.startReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.startReportingTask({
                    request: {
                        reportingTask: mockReportingTaskEntity
                    }
                })
            );
        });

        it('should dispatch stopReportingTask action when stopReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.stopReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.stopReportingTask({
                    request: {
                        reportingTask: mockReportingTaskEntity
                    }
                })
            );
        });

        it('should dispatch openChangeReportingTaskVersionDialog action when changeReportingTaskVersion is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.changeReportingTaskVersion(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.openChangeReportingTaskVersionDialog({
                    request: {
                        id: mockReportingTaskEntity.id,
                        bundle: mockReportingTaskEntity.component.bundle,
                        uri: mockReportingTaskEntity.uri,
                        type: mockReportingTaskEntity.component.type,
                        revision: mockReportingTaskEntity.revision
                    }
                })
            );
        });

        it('should dispatch clearReportingTaskBulletins action when clearBulletinsReportingTask is called', async () => {
            const { component, store } = await setup();
            const mockReportingTaskEntity = createMockReportingTaskEntity();
            jest.spyOn(store, 'dispatch');

            component.clearBulletinsReportingTask(mockReportingTaskEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ReportingTasksActions.clearReportingTaskBulletins({
                    request: {
                        uri: mockReportingTaskEntity.uri,
                        fromTimestamp: '2023-10-08T12:00:00.000Z',
                        componentId: mockReportingTaskEntity.id,
                        componentType: ComponentType.ReportingTask
                    }
                })
            );
        });
    });
});
