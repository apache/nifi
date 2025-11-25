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
import { RegistryClientTable } from './registry-client-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { BulletinEntity } from '@nifi/shared';
import { RegistryClientEntity } from '../../../../../state/shared';

describe('RegistryClientTable', () => {
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
                sourceType: 'REGISTRY_CLIENT',
                sourceName: 'Test Registry Client',
                level: 'INFO',
                message: `Test bulletin message ${id}`,
                timestamp: timestamp,
                timestampIso: timestamp
            }
        };
    }

    function createMockRegistryClientEntity(): RegistryClientEntity {
        return {
            id: 'test-registry-client-id',
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
                id: 'test-registry-client-id',
                name: 'Test Registry Client',
                description: 'Test Description',
                type: 'org.apache.nifi.TestRegistryClient',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                properties: {},
                descriptors: {},
                validationErrors: [],
                validationStatus: 'VALID'
            },
            bulletins: []
        };
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [RegistryClientTable, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(RegistryClientTable);
        const component = fixture.componentInstance;

        // Set up required inputs
        component.selectedRegistryClientId = '';

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
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.canClearBulletins(mockEntity)).toBe(true);
        });

        it('should return false when user cannot write even if bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user can write but no bulletins are present (empty array)', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = [];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user can write but bulletins property is undefined', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = true;
            mockEntity.bulletins = undefined as any;

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return false when user cannot write and no bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = false;
            mockEntity.bulletins = [];

            expect(component.canClearBulletins(mockEntity)).toBe(false);
        });

        it('should return true when user can write and multiple bulletins are present', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
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
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;

            expect(component.canRead(mockEntity)).toBe(true);
        });

        it('should return false for canRead when entity lacks read permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = false;

            expect(component.canRead(mockEntity)).toBe(false);
        });

        it('should return true for canWrite when entity has write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = true;

            expect(component.canWrite(mockEntity)).toBe(true);
        });

        it('should return false for canWrite when entity lacks write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canWrite = false;

            expect(component.canWrite(mockEntity)).toBe(false);
        });

        it('should return true for canConfigure when entity has both read and write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = true;

            expect(component.canConfigure(mockEntity)).toBe(true);
        });

        it('should return false for canConfigure when entity lacks write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = false;

            expect(component.canConfigure(mockEntity)).toBe(false);
        });

        it('should return true for canDelete when entity has both read and write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = true;

            expect(component.canDelete(mockEntity)).toBe(true);
        });

        it('should return false for canDelete when entity lacks write permissions', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.permissions.canWrite = false;

            expect(component.canDelete(mockEntity)).toBe(false);
        });
    });

    describe('Entity State Methods', () => {
        it('should return true for hasErrors when entity has validation errors and can be read', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.component.validationErrors = ['Error 1', 'Error 2'];

            expect(component.hasErrors(mockEntity)).toBe(true);
        });

        it('should return false for hasErrors when entity has no validation errors', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.component.validationErrors = [];

            expect(component.hasErrors(mockEntity)).toBe(false);
        });

        it('should return false for hasErrors when entity cannot be read', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = false;
            mockEntity.component.validationErrors = ['Error 1'];

            expect(component.hasErrors(mockEntity)).toBe(false);
        });

        it('should return true for hasBulletins when entity has bulletins and can be read', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.hasBulletins(mockEntity)).toBe(true);
        });

        it('should return false for hasBulletins when entity has no bulletins', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = true;
            mockEntity.bulletins = [];

            expect(component.hasBulletins(mockEntity)).toBe(false);
        });

        it('should return false for hasBulletins when entity cannot be read', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            mockEntity.permissions.canRead = false;
            mockEntity.bulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];

            expect(component.hasBulletins(mockEntity)).toBe(false);
        });
    });

    describe('Bulletin Methods', () => {
        it('should return correct bulletins tip data', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            const testBulletins = [createTestBulletin(1, '2023-01-01T12:00:00.000Z')];
            mockEntity.bulletins = testBulletins;

            const tipData = component.getBulletinsTipData(mockEntity);

            expect(tipData.bulletins).toBe(testBulletins);
        });
    });

    describe('Validation Methods', () => {
        it('should return correct validation errors tip data', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            const validationErrors = ['Error 1', 'Error 2'];
            mockEntity.component.validationErrors = validationErrors;

            const tipData = component.getValidationErrorsTipData(mockEntity);

            expect(tipData.isValidating).toBe(false);
            expect(tipData.validationErrors).toBe(validationErrors);
        });
    });

    describe('Formatting Methods', () => {
        it('should format type correctly', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();

            const formattedType = component.formatType(mockEntity);

            expect(formattedType).toBe('TestRegistryClient 1.0.0');
        });

        it('should format bundle correctly', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();

            const formattedBundle = component.formatBundle(mockEntity);

            expect(formattedBundle).toBe('org.apache.nifi - test-bundle');
        });
    });

    describe('Selection Methods', () => {
        it('should return true for isSelected when entity ID matches selected ID', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            component.selectedRegistryClientId = mockEntity.id;

            expect(component.isSelected(mockEntity as any)).toBe(true);
        });

        it('should return false for isSelected when entity ID does not match selected ID', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            component.selectedRegistryClientId = 'different-id';

            expect(component.isSelected(mockEntity as any)).toBe(false);
        });

        it('should return false for isSelected when no ID is selected', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            component.selectedRegistryClientId = '';

            expect(component.isSelected(mockEntity as any)).toBe(false);
        });
    });

    describe('Sorting Methods', () => {
        it('should update sort and re-sort data', async () => {
            const { component } = await setup();
            const mockEntities = [
                {
                    ...createMockRegistryClientEntity(),
                    component: { ...createMockRegistryClientEntity().component, name: 'B Registry Client' }
                },
                {
                    ...createMockRegistryClientEntity(),
                    component: { ...createMockRegistryClientEntity().component, name: 'A Registry Client' }
                }
            ];
            component.dataSource.data = mockEntities;

            const newSort = { active: 'name', direction: 'asc' as const };
            component.updateSort(newSort);

            expect(component.sort).toBe(newSort);
            expect(component.dataSource.data[0].component.name).toBe('A Registry Client');
            expect(component.dataSource.data[1].component.name).toBe('B Registry Client');
        });

        it('should sort entities by name in descending order', async () => {
            const { component } = await setup();
            const mockEntities = [
                {
                    ...createMockRegistryClientEntity(),
                    component: { ...createMockRegistryClientEntity().component, name: 'A Registry Client' }
                },
                {
                    ...createMockRegistryClientEntity(),
                    component: { ...createMockRegistryClientEntity().component, name: 'B Registry Client' }
                }
            ];

            const sortedEntities = component.sortEvents(mockEntities, { active: 'name', direction: 'desc' });

            expect(sortedEntities[0].component.name).toBe('B Registry Client');
            expect(sortedEntities[1].component.name).toBe('A Registry Client');
        });
    });

    describe('Event Emitters', () => {
        it('should emit selectRegistryClient when select is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            jest.spyOn(component.selectRegistryClient, 'next');

            component.select(mockEntity as any);

            expect(component.selectRegistryClient.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit configureRegistryClient when configureClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            jest.spyOn(component.configureRegistryClient, 'next');

            component.configureClicked(mockEntity);

            expect(component.configureRegistryClient.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit deleteRegistryClient when deleteClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            jest.spyOn(component.deleteRegistryClient, 'next');

            component.deleteClicked(mockEntity);

            expect(component.deleteRegistryClient.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit clearBulletinsRegistryClient when clearBulletinsClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            jest.spyOn(component.clearBulletinsRegistryClient, 'next');

            component.clearBulletinsClicked(mockEntity);

            expect(component.clearBulletinsRegistryClient.next).toHaveBeenCalledWith(mockEntity);
        });

        it('should emit viewRegistryClientDocumentation when viewDocumentationClicked is called', async () => {
            const { component } = await setup();
            const mockEntity = createMockRegistryClientEntity();
            jest.spyOn(component.viewRegistryClientDocumentation, 'next');

            component.viewDocumentationClicked(mockEntity);

            expect(component.viewRegistryClientDocumentation.next).toHaveBeenCalledWith(mockEntity);
        });
    });
});
