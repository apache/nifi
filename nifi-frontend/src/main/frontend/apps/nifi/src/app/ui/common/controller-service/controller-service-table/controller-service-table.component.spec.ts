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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ControllerServiceTable } from './controller-service-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ControllerServiceEntity } from '../../../../state/shared';
import { BulletinEntity } from '@nifi/shared';
import { CurrentUser } from '../../../../state/current-user';
import { FlowConfiguration } from '../../../../state/flow-configuration';

describe('ControllerServiceTable', () => {
    // Mock data factories
    function createTestBulletin(id: number, timestamp: string): BulletinEntity {
        return {
            id,
            canRead: true,
            timestampIso: new Date().toISOString(),
            sourceId: 'test-controller-service-id',
            groupId: 'test-group',
            timestamp,
            bulletin: {
                id,
                nodeAddress: 'localhost',
                category: 'test',
                groupId: 'test-group',
                sourceId: 'test-controller-service-id',
                sourceName: 'Test Controller Service',
                sourceType: 'COMPONENT',
                level: 'ERROR',
                message: 'Test error message',
                timestamp,
                timestampIso: timestamp
            }
        };
    }

    function createMockControllerServiceEntity(): ControllerServiceEntity {
        return {
            id: 'test-controller-service-id',
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
                id: 'test-controller-service-id',
                name: 'Test Controller Service',
                type: 'org.apache.nifi.TestControllerService',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
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
            },
            bulletins: []
        } as ControllerServiceEntity;
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [NoopAnimationsModule, ControllerServiceTable]
        }).compileComponents();

        const fixture = TestBed.createComponent(ControllerServiceTable);
        const component = fixture.componentInstance;
        fixture.detectChanges();

        return { component, fixture };
    }

    interface TableSetupOptions {
        readOnly?: boolean;
        controllerServices?: ControllerServiceEntity[];
        canModifyParent?: (entity: ControllerServiceEntity) => boolean;
        canManageAccessPolicies?: boolean;
    }

    async function setupTable(options: TableSetupOptions = {}): Promise<{
        fixture: ComponentFixture<ControllerServiceTable>;
        component: ControllerServiceTable;
    }> {
        const { fixture, component } = await setup();

        component.readOnly = options.readOnly ?? false;
        component.canModifyParent = options.canModifyParent ?? (() => false);
        component.formatScope = () => '';
        component.definedByCurrentGroup = () => true;
        component.flowConfiguration = {
            supportsManagedAuthorizer: options.canManageAccessPolicies ?? true
        } as FlowConfiguration;
        component.currentUser = {
            tenantsPermissions: { canRead: options.canManageAccessPolicies ?? true }
        } as CurrentUser;
        component.controllerServices = options.controllerServices ?? [createMockControllerServiceEntity()];

        fixture.detectChanges();
        return { fixture, component };
    }

    function getMenuItemTexts(fixture: ComponentFixture<ControllerServiceTable>): string[] {
        const trigger = fixture.nativeElement.querySelector('button.global-menu') as HTMLElement | null;
        trigger?.click();
        fixture.detectChanges();

        const items = document.querySelectorAll('.mat-mdc-menu-item');
        return Array.from(items).map((el) => (el.textContent ?? '').trim().replace(/\s+/g, ' '));
    }

    function closeAllMenus(): void {
        document.querySelectorAll('.cdk-overlay-backdrop').forEach((backdrop) => {
            (backdrop as HTMLElement).click();
        });
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    describe('canClearBulletins', () => {
        it('should return true when user can write and bulletins are present', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            // Arrange
            const testBulletin = createTestBulletin(1, '12:00:00 UTC');
            mockControllerServiceEntity.bulletins = [testBulletin];
            mockControllerServiceEntity.permissions.canWrite = true;

            // Act
            const result = component.canClearBulletins(mockControllerServiceEntity);

            // Assert
            expect(result).toBe(true);
        });

        it('should return false when user cannot write even if bulletins are present', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            // Arrange
            const testBulletin = createTestBulletin(1, '12:00:00 UTC');
            mockControllerServiceEntity.bulletins = [testBulletin];
            mockControllerServiceEntity.permissions.canWrite = false;

            // Act
            const result = component.canClearBulletins(mockControllerServiceEntity);

            // Assert
            expect(result).toBe(false);
        });

        it('should return false when user can write but no bulletins are present (empty array)', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            // Arrange
            mockControllerServiceEntity.bulletins = [];
            mockControllerServiceEntity.permissions.canWrite = true;

            // Act
            const result = component.canClearBulletins(mockControllerServiceEntity);

            // Assert
            expect(result).toBe(false);
        });

        it('should return false when user can write but bulletins property is undefined', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            // Arrange
            mockControllerServiceEntity.bulletins = undefined as any;
            mockControllerServiceEntity.permissions.canWrite = true;

            // Act
            const result = component.canClearBulletins(mockControllerServiceEntity);

            // Assert
            expect(result).toBe(false);
        });

        it('should return false when user cannot write and no bulletins are present', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            // Arrange
            mockControllerServiceEntity.bulletins = [];
            mockControllerServiceEntity.permissions.canWrite = false;

            // Act
            const result = component.canClearBulletins(mockControllerServiceEntity);

            // Assert
            expect(result).toBe(false);
        });

        it('should return true when user can write and multiple bulletins are present', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            // Arrange
            const testBulletins: BulletinEntity[] = [
                createTestBulletin(1, '12:00:00 UTC'),
                {
                    id: 2,
                    canRead: true,
                    timestampIso: '2023-01-01T12:01:00.000Z',
                    sourceId: 'test-controller-service-id',
                    groupId: 'test-group',
                    timestamp: '12:01:00 UTC',
                    bulletin: {
                        id: 2,
                        nodeAddress: 'localhost',
                        category: 'test',
                        groupId: 'test-group',
                        sourceId: 'test-controller-service-id',
                        sourceName: 'Test Controller Service',
                        sourceType: 'COMPONENT',
                        level: 'WARN',
                        message: 'Second warning message',
                        timestamp: '12:01:00 UTC',
                        timestampIso: '2023-01-01T12:01:00.000Z'
                    }
                }
            ];
            mockControllerServiceEntity.bulletins = testBulletins;
            mockControllerServiceEntity.permissions.canWrite = true;

            // Act
            const result = component.canClearBulletins(mockControllerServiceEntity);

            // Assert
            expect(result).toBe(true);
        });
    });

    describe('readOnly mode', () => {
        afterEach(() => {
            closeAllMenus();
        });

        it('should default readOnly to false', async () => {
            const { component } = await setup();
            expect(component.readOnly).toBe(false);
        });

        it('should restrict the action menu to view-only entries when readOnly is true', async () => {
            const entity = createMockControllerServiceEntity();
            entity.status.runStatus = 'DISABLED';
            entity.component.persistsState = true;
            entity.bulletins = [createTestBulletin(1, '12:00:00 UTC')];
            entity.component.customUiUrl = 'https://example.com/advanced';
            entity.component.multipleVersionsAvailable = true;

            const { fixture } = await setupTable({
                readOnly: true,
                controllerServices: [entity]
            });

            const labels = getMenuItemTexts(fixture);
            expect(labels).toEqual(['View Configuration', 'View State', 'View Documentation']);
        });

        it('should omit View State when persistsState is false even in readOnly mode', async () => {
            const entity = createMockControllerServiceEntity();
            entity.status.runStatus = 'DISABLED';
            entity.component.persistsState = false;

            const { fixture } = await setupTable({
                readOnly: true,
                controllerServices: [entity]
            });

            const labels = getMenuItemTexts(fixture);
            expect(labels).toEqual(['View Configuration', 'View Documentation']);
        });

        it('should render no menu items in readOnly mode when the user cannot read the entity', async () => {
            const entity = createMockControllerServiceEntity();
            entity.permissions.canRead = false;

            const { fixture } = await setupTable({
                readOnly: true,
                controllerServices: [entity]
            });

            expect(getMenuItemTexts(fixture)).toEqual([]);
        });

        it('should render the full mutating action menu when readOnly is false', async () => {
            const entity = createMockControllerServiceEntity();
            entity.status.runStatus = 'DISABLED';

            const { fixture } = await setupTable({
                readOnly: false,
                controllerServices: [entity]
            });

            const labels = getMenuItemTexts(fixture);
            expect(labels).toContain('Edit');
            expect(labels).toContain('Enable');
            expect(labels).toContain('View Documentation');
        });
    });
});
