/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestBed } from '@angular/core/testing';
import { ParameterProvidersTable } from './parameter-providers-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ParameterProviderEntity } from '../../../state/parameter-providers';
import { BulletinEntity } from '@nifi/shared';
import { CurrentUser } from '../../../../../state/current-user';
import { FlowConfiguration } from '../../../../../state/flow-configuration';

describe('ParameterProvidersTable', () => {
    // Mock data factories
    function createTestBulletin(id: number, timestamp: string): BulletinEntity {
        return {
            id,
            canRead: true,
            timestampIso: new Date().toISOString(),
            sourceId: 'test-parameter-provider-id',
            groupId: 'test-group',
            timestamp,
            bulletin: {
                id,
                nodeAddress: 'localhost',
                category: 'test',
                groupId: 'test-group',
                sourceId: 'test-parameter-provider-id',
                sourceName: 'Test Parameter Provider',
                sourceType: 'COMPONENT',
                level: 'ERROR',
                message: `Test error message ${id}`,
                timestamp,
                timestampIso: timestamp
            }
        };
    }

    function createMockParameterProviderEntity(): ParameterProviderEntity {
        return {
            id: 'test-parameter-provider-id',
            uri: 'test-uri',
            permissions: {
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 1,
                clientId: 'test-client'
            },
            component: {
                id: 'test-parameter-provider-id',
                name: 'Test Parameter Provider',
                type: 'org.apache.nifi.TestParameterProvider',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
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
                validationStatus: 'VALID',
                customUiUrl: ''
            },
            bulletins: []
        } as ParameterProviderEntity;
    }

    function createMockCurrentUser(canRead: boolean = true, canWrite: boolean = true): CurrentUser {
        return {
            identity: 'test-user',
            anonymous: false,
            logoutSupported: true,
            provenancePermissions: {
                canRead: true,
                canWrite: true
            },
            countersPermissions: {
                canRead: true,
                canWrite: true
            },
            tenantsPermissions: {
                canRead: true,
                canWrite: true
            },
            controllerPermissions: {
                canRead,
                canWrite
            },
            policiesPermissions: {
                canRead: true,
                canWrite: true
            },
            systemPermissions: {
                canRead: true,
                canWrite: true
            },
            parameterContextPermissions: {
                canRead: true,
                canWrite: true
            },
            restrictedComponentsPermissions: {
                canRead: true,
                canWrite: true
            },
            componentRestrictionPermissions: [],
            canVersionFlows: true
        } as CurrentUser;
    }

    function createMockFlowConfiguration(supportsManagedAuthorizer: boolean = true): FlowConfiguration {
        return {
            supportsManagedAuthorizer,
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
            imports: [ParameterProvidersTable, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(ParameterProvidersTable);
        const component = fixture.componentInstance;

        // Set up required inputs
        component.currentUser = createMockCurrentUser();
        component.flowConfiguration = createMockFlowConfiguration();
        component.selectedParameterProviderId = '';

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
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.canClearBulletins(mockEntity)).toBe(true);
        });

        it('should return false when user cannot write even if bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user can write but no bulletins are present (empty array)', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user can write but bulletins property is undefined', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = undefined as any;

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user cannot write and no bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.bulletins = [];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return true when user can write and multiple bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
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
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canRead = true;

            expect(component.canRead(mockEntity)).toBe(true);
        });

        it('should return false for canRead when entity lacks read permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canRead = false;

            expect(component.canRead(mockEntity)).toBe(false);
        });

        it('should return true for canWrite when entity has write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = true;

            expect(component.canWrite(mockEntity)).toBe(true);
        });

        it('should return false for canWrite when entity lacks write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            mockEntity.permissions.canWrite = false;

            expect(component.canWrite(mockEntity)).toBe(false);
        });
    });

    describe('Event Emitters', () => {
        it('should emit clearBulletinsParameterProvider when clearBulletinsClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockParameterProviderEntity();
            jest.spyOn(component.clearBulletinsParameterProvider, 'next');

            component.clearBulletinsClicked(mockEntity);

            expect(component.clearBulletinsParameterProvider.next).toHaveBeenCalledWith(mockEntity);
        });
    });
});
