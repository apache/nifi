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
import { ReportingTaskTable } from './reporting-task-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ReportingTaskEntity } from '../../../state/reporting-tasks';
import { BulletinEntity } from '@nifi/shared';
import { CurrentUser } from '../../../../../state/current-user';
import { FlowConfiguration } from '../../../../../state/flow-configuration';

describe('ReportingTaskTable', () => {
    // Mock data factories
    function createTestBulletin(id: number, timestamp: string): BulletinEntity {
        return {
            canRead: true,
            id: id,
            timestampIso: new Date().toISOString(),
            sourceId: 'test-source-id',
            groupId: 'test-group-id',
            timestamp: timestamp,
            bulletin: {
                id: id,
                category: 'Test Category',
                groupId: 'test-group-id',
                sourceId: 'test-source-id',
                sourceType: 'REPORTING_TASK',
                sourceName: 'Test Reporting Task',
                level: 'INFO',
                message: `Test bulletin message ${id}`,
                timestamp: timestamp,
                timestampIso: timestamp
            }
        };
    }

    function createMockReportingTaskEntity(): ReportingTaskEntity {
        return {
            id: 'test-reporting-task-id',
            uri: 'test-uri',
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
            },
            bulletins: []
        };
    }

    function createMockCurrentUser(): CurrentUser {
        return {
            identity: 'test-user',
            anonymous: false,
            canVersionFlows: true,
            logoutSupported: true,
            tenantsPermissions: {
                canRead: true,
                canWrite: true
            },
            countersPermissions: {
                canRead: true,
                canWrite: true
            },
            policiesPermissions: {
                canRead: true,
                canWrite: true
            },
            provenancePermissions: {
                canRead: true,
                canWrite: true
            },
            restrictedComponentsPermissions: {
                canRead: true,
                canWrite: true
            },
            componentRestrictionPermissions: [],
            systemPermissions: {
                canRead: true,
                canWrite: true
            },
            parameterContextPermissions: {
                canRead: true,
                canWrite: true
            },
            controllerPermissions: {
                canRead: true,
                canWrite: true
            }
        };
    }

    function createMockFlowConfiguration(): FlowConfiguration {
        return {
            supportsManagedAuthorizer: true,
            supportsConfigurableAuthorizer: true,
            supportsConfigurableUsersAndGroups: true,
            supportsRestrictedComponents: true,
            maxTimerDrivenThreadCount: 10,
            maxEventDrivenThreadCount: 5,
            timeOffset: 0,
            currentTime: '2023-01-01 12:00:00 EST',
            defaultBackPressureObjectThreshold: 10000,
            defaultBackPressureDataSizeThreshold: 1073741824
        } as FlowConfiguration;
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [ReportingTaskTable, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(ReportingTaskTable);
        const component = fixture.componentInstance;

        // Set up required inputs
        component.selectedReportingTaskId = '';
        component.currentUser = createMockCurrentUser();
        component.flowConfiguration = createMockFlowConfiguration();

        fixture.detectChanges();

        return { component, fixture };
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    describe('canClearBulletins', () => {
        it('should return true when user can write and bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.canClearBulletins(mockEntity)).toBe(true);
        });

        it('should return false when user cannot write even if bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user can write but no bulletins are present (empty array)', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user can write but bulletins property is undefined', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = undefined as any;

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user cannot write and no bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.bulletins = [];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return true when user can write and multiple bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [
                createTestBulletin(1, '2023-01-01T12:00:00.000Z'),
                createTestBulletin(2, '2023-01-01T12:01:00.000Z')
            ];

            expect(component.canClearBulletins(mockEntity)).toBe(true);
        });
    });

    describe('Permission Methods', () => {
        it('should return true for canRead when entity has read permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;

            expect(component.canRead(mockEntity)).toBe(true);
        });

        it('should return false for canRead when entity lacks read permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = false;

            expect(component.canRead(mockEntity)).toBe(false);
        });

        it('should return true for canWrite when entity has write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;

            expect(component.canWrite(mockEntity)).toBe(true);
        });

        it('should return false for canWrite when entity lacks write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = false;

            expect(component.canWrite(mockEntity)).toBe(false);
        });

        it('should return true for canOperate when entity has write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;

            expect(component.canOperate(mockEntity)).toBe(true);
        });

        it('should return true for canOperate when entity has operate permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.operatePermissions = { canRead: true, canWrite: true };

            expect(component.canOperate(mockEntity)).toBe(true);
        });

        it('should return false for canOperate when entity lacks both write and operate permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.operatePermissions = { canRead: true, canWrite: false };

            expect(component.canOperate(mockEntity)).toBe(false);
        });
    });

    describe('State Methods', () => {
        it('should return true for isRunning when status is RUNNING', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.runStatus = 'RUNNING';

            expect(component.isRunning(mockEntity)).toBe(true);
        });

        it('should return false for isRunning when status is STOPPED', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.runStatus = 'STOPPED';

            expect(component.isRunning(mockEntity)).toBe(false);
        });

        it('should return true for isStopped when status is STOPPED', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.runStatus = 'STOPPED';

            expect(component.isStopped(mockEntity)).toBe(true);
        });

        it('should return true for isDisabled when status is DISABLED', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.runStatus = 'DISABLED';

            expect(component.isDisabled(mockEntity)).toBe(true);
        });

        it('should return true for isValid when validation status is VALID', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.validationStatus = 'VALID';

            expect(component.isValid(mockEntity)).toBe(true);
        });

        it('should return true for isStoppedOrDisabled when stopped', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.runStatus = 'STOPPED';

            expect(component.isStoppedOrDisabled(mockEntity)).toBe(true);
        });

        it('should return true for isStoppedOrDisabled when disabled', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.runStatus = 'DISABLED';

            expect(component.isStoppedOrDisabled(mockEntity)).toBe(true);
        });
    });

    describe('Action Permission Methods', () => {
        it('should return true for canStart when entity can operate, is stopped, and is valid', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'STOPPED';
            mockEntity.status.validationStatus = 'VALID';

            expect(component.canStart(mockEntity)).toBe(true);
        });

        it('should return false for canStart when entity is running', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'RUNNING';
            mockEntity.status.validationStatus = 'VALID';

            expect(component.canStart(mockEntity)).toBe(false);
        });

        it('should return true for canStop when entity can operate and is running', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'RUNNING';

            expect(component.canStop(mockEntity)).toBe(true);
        });

        it('should return true for canEdit when entity can read, write, and is stopped or disabled', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'STOPPED';

            expect(component.canEdit(mockEntity)).toBe(true);
        });

        it('should return false for canEdit when entity is running', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'RUNNING';

            expect(component.canEdit(mockEntity)).toBe(false);
        });

        it('should return true for canDelete when entity meets all delete criteria', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'STOPPED';
            component.currentUser.controllerPermissions.canRead = true;
            component.currentUser.controllerPermissions.canWrite = true;

            expect(component.canDelete(mockEntity)).toBe(true);
        });

        it('should return false for canDelete when user lacks controller write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = true;
            mockEntity.status.runStatus = 'STOPPED';
            component.currentUser.controllerPermissions.canWrite = false;

            expect(component.canDelete(mockEntity)).toBe(false);
        });
    });

    describe('Formatting Methods', () => {
        it('should format name correctly when can read', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.component.name = 'Test Task Name';

            expect(component.formatName(mockEntity)).toBe('Test Task Name');
        });

        it('should return id when cannot read', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = false;
            mockEntity.id = 'test-id';

            expect(component.formatName(mockEntity)).toBe('test-id');
        });

        it('should format type correctly when can read', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;

            const formattedType = component.formatType(mockEntity);

            expect(formattedType).toBe('TestReportingTask 1.0.0');
        });

        it('should return empty string for type when cannot read', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = false;

            expect(component.formatType(mockEntity)).toBe('');
        });

        it('should format bundle correctly when can read', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;

            const formattedBundle = component.formatBundle(mockEntity);

            expect(formattedBundle).toBe('org.apache.nifi - test-bundle');
        });

        it('should return empty string for bundle when cannot read', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = false;

            expect(component.formatBundle(mockEntity)).toBe('');
        });

        it('should format state correctly for different statuses', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();

            mockEntity.status.validationStatus = 'VALIDATING';
            expect(component.formatState(mockEntity)).toBe('Validating');

            mockEntity.status.validationStatus = 'INVALID';
            expect(component.formatState(mockEntity)).toBe('Invalid');

            mockEntity.status.validationStatus = 'VALID';
            mockEntity.status.runStatus = 'RUNNING';
            expect(component.formatState(mockEntity)).toBe('Running');

            mockEntity.status.runStatus = 'STOPPED';
            expect(component.formatState(mockEntity)).toBe('Stopped');

            mockEntity.status.runStatus = 'DISABLED';
            expect(component.formatState(mockEntity)).toBe('Disabled');
        });
    });

    describe('Entity State Methods', () => {
        it('should return true for hasComments when comments are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.component.comments = 'Test comments';

            expect(component.hasComments(mockEntity)).toBe(true);
        });

        it('should return false for hasComments when comments are blank', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.component.comments = '';

            expect(component.hasComments(mockEntity)).toBe(false);
        });

        it('should return true for hasErrors when validation errors are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.component.validationErrors = ['Error 1', 'Error 2'];

            expect(component.hasErrors(mockEntity)).toBe(true);
        });

        it('should return false for hasErrors when no validation errors', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.component.validationErrors = [];

            expect(component.hasErrors(mockEntity)).toBe(false);
        });

        it('should return true for hasBulletins when bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.hasBulletins(mockEntity)).toBe(true);
        });

        it('should return false for hasBulletins when no bulletins', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.bulletins = [];

            expect(component.hasBulletins(mockEntity)).toBe(false);
        });

        it('should return true for hasActiveThreads when active thread count > 0', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.activeThreadCount = 2;

            expect(component.hasActiveThreads(mockEntity)).toBe(true);
        });

        it('should return false for hasActiveThreads when active thread count is 0', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.status.activeThreadCount = 0;

            expect(component.hasActiveThreads(mockEntity)).toBe(false);
        });

        it('should return true for hasAdvancedUi when can read and customUiUrl is present', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.component.customUiUrl = 'http://example.com/ui';

            expect(component.hasAdvancedUi(mockEntity)).toBe(true);
        });

        it('should return false for hasAdvancedUi when customUiUrl is empty', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.component.customUiUrl = '';

            expect(component.hasAdvancedUi(mockEntity)).toBe(false);
        });
    });

    describe('Selection Methods', () => {
        it('should return true for isSelected when entity ID matches selected ID', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            component.selectedReportingTaskId = mockEntity.id;

            expect(component.isSelected(mockEntity)).toBe(true);
        });

        it('should return false for isSelected when entity ID does not match selected ID', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            component.selectedReportingTaskId = 'different-id';

            expect(component.isSelected(mockEntity)).toBe(false);
        });

        it('should return false for isSelected when no ID is selected', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            component.selectedReportingTaskId = '';

            expect(component.isSelected(mockEntity)).toBe(false);
        });
    });

    describe('Event Emitters', () => {
        it('should emit selectReportingTask when select is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            jest.spyOn(component.selectReportingTask, 'next');

            component.select(mockEntity);

            expect(component.selectReportingTask.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit clearBulletinsReportingTask when clearBulletinsClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            jest.spyOn(component.clearBulletinsReportingTask, 'next');

            component.clearBulletinsClicked(mockEntity);

            expect(component.clearBulletinsReportingTask.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit configureReportingTask when configureClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            jest.spyOn(component.configureReportingTask, 'next');

            component.configureClicked(mockEntity);

            expect(component.configureReportingTask.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit deleteReportingTask when deleteClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            jest.spyOn(component.deleteReportingTask, 'next');

            component.deleteClicked(mockEntity);

            expect(component.deleteReportingTask.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit startReportingTask when startClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            jest.spyOn(component.startReportingTask, 'next');

            component.startClicked(mockEntity);

            expect(component.startReportingTask.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit stopReportingTask when stopClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockReportingTaskEntity();
            jest.spyOn(component.stopReportingTask, 'next');

            component.stopClicked(mockEntity);

            expect(component.stopReportingTask.next).toHaveBeenCalledWith(mockEntity);
        });
    });
});
