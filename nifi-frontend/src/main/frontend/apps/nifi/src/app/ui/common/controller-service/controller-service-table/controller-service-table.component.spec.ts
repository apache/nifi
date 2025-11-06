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
import { ControllerServiceTable } from './controller-service-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ControllerServiceEntity } from '../../../../state/shared';
import { BulletinEntity } from '@nifi/shared';

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
});
