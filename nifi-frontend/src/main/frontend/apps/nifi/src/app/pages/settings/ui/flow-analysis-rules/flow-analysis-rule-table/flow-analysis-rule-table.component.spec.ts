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
import { MatTableModule } from '@angular/material/table';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Sort } from '@angular/material/sort';

import { FlowAnalysisRuleTable } from './flow-analysis-rule-table.component';
import { FlowAnalysisRuleEntity } from '../../../state/flow-analysis-rules';
import { BulletinEntity } from '@nifi/shared';
import { CurrentUser } from '../../../../../state/current-user';

describe('FlowAnalysisRuleTable', () => {
    // Mock data factories
    function createTestBulletin(): BulletinEntity {
        return {
            canRead: true,
            id: 1,
            timestampIso: new Date().toISOString(),
            sourceId: 'test-source',
            groupId: 'test-group',
            timestamp: '12:00:00 UTC',
            bulletin: {
                id: 1,
                nodeAddress: 'localhost:8080',
                category: 'INFO',
                groupId: 'test-group',
                sourceId: 'test-source',
                sourceName: 'Test Source',
                level: 'INFO',
                message: 'Test bulletin message',
                timestamp: '12:00:00 UTC',
                timestampIso: '2023-10-08T12:00:00.000Z',
                sourceType: 'FLOW_ANALYSIS_RULE'
            }
        };
    }

    function createMockFlowAnalysisRuleEntity(): FlowAnalysisRuleEntity {
        return {
            id: 'test-flow-analysis-rule-id',
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
                id: 'test-flow-analysis-rule-id',
                name: 'Test Flow Analysis Rule',
                type: 'org.apache.nifi.TestFlowAnalysisRule',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-bundle',
                    version: '1.0.0'
                },
                state: 'DISABLED',
                comments: 'Test comments',
                persistsState: true,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                properties: {},
                descriptors: {},
                customUiUrl: '',
                annotationData: '',
                validationErrors: [],
                validationStatus: 'VALID'
            },
            status: {
                runStatus: 'DISABLED',
                validationStatus: 'VALID'
            },
            bulletins: []
        };
    }

    function createMockCurrentUser(): CurrentUser {
        return {
            identity: 'test-user',
            anonymous: false,
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
                canRead: true,
                canWrite: true
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
            canVersionFlows: true,
            logoutSupported: true
        };
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [NoopAnimationsModule, MatTableModule, FlowAnalysisRuleTable]
        }).compileComponents();

        const fixture = TestBed.createComponent(FlowAnalysisRuleTable);
        const component = fixture.componentInstance;

        // Set required inputs
        component.currentUser = createMockCurrentUser();
        component.selectedFlowAnalysisRuleId = '';

        fixture.detectChanges();

        return { component, fixture };
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    describe('Permission methods', () => {
        it('should return true for canRead when entity has read permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canRead = true;

            expect(component.canRead(entity)).toBe(true);
        });

        it('should return false for canRead when entity lacks read permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canRead = false;

            expect(component.canRead(entity)).toBe(false);
        });

        it('should return true for canWrite when entity has write permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = true;

            expect(component.canWrite(entity)).toBe(true);
        });

        it('should return false for canWrite when entity lacks write permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = false;

            expect(component.canWrite(entity)).toBe(false);
        });

        it('should return true for canOperate when entity has write permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = true;

            expect(component.canOperate(entity)).toBe(true);
        });

        it('should return true for canOperate when entity has operate permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = false;
            entity.operatePermissions = { canRead: true, canWrite: true };

            expect(component.canOperate(entity)).toBe(true);
        });

        it('should return false for canOperate when entity lacks both write and operate permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = false;
            entity.operatePermissions = { canRead: true, canWrite: false };

            expect(component.canOperate(entity)).toBe(false);
        });

        it('should return true for canModifyParent when user has controller permissions', async () => {
            const { component } = await setup();
            component.currentUser.controllerPermissions = { canRead: true, canWrite: true };

            expect(component.canModifyParent()).toBe(true);
        });

        it('should return false for canModifyParent when user lacks controller permissions', async () => {
            const { component } = await setup();
            component.currentUser.controllerPermissions = { canRead: false, canWrite: false };

            expect(component.canModifyParent()).toBe(false);
        });
    });

    describe('Entity state methods', () => {
        it('should return true for isDisabled when rule status is DISABLED', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';

            expect(component.isDisabled(entity)).toBe(true);
        });

        it('should return false for isDisabled when rule status is not DISABLED', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';

            expect(component.isDisabled(entity)).toBe(false);
        });

        it('should return true for isEnabled when rule status is ENABLED', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';

            expect(component.isEnabled(entity)).toBe(true);
        });

        it('should return false for isEnabled when rule status is not ENABLED', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';

            expect(component.isEnabled(entity)).toBe(false);
        });

        it('should return true for hasComments when entity has comments', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.comments = 'Test comments';

            expect(component.hasComments(entity)).toBe(true);
        });

        it('should return false for hasComments when entity has no comments', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.comments = '';

            expect(component.hasComments(entity)).toBe(false);
        });

        it('should return true for hasErrors when entity has validation errors', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.validationErrors = ['Error 1', 'Error 2'];

            expect(component.hasErrors(entity)).toBe(true);
        });

        it('should return false for hasErrors when entity has no validation errors', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.validationErrors = [];

            expect(component.hasErrors(entity)).toBe(false);
        });
    });

    describe('Bulletin methods', () => {
        it('should return true for hasBulletins when entity has bulletins', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.bulletins = [createTestBulletin()];

            expect(component.hasBulletins(entity)).toBe(true);
        });

        it('should return false for hasBulletins when entity has no bulletins', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.bulletins = [];

            expect(component.hasBulletins(entity)).toBe(false);
        });

        it('should return false for hasBulletins when bulletins is undefined', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.bulletins = undefined as any;

            expect(component.hasBulletins(entity)).toBe(false);
        });

        it('should return correct bulletins tip data', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            const bulletins = [createTestBulletin()];
            entity.bulletins = bulletins;

            const tipData = component.getBulletinsTipData(entity);

            expect(tipData.bulletins).toEqual(bulletins);
        });

        it('should return correct validation errors tip data', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.validationErrors = ['Error 1'];
            entity.status.validationStatus = 'VALIDATING';

            const tipData = component.getValidationErrorsTipData(entity);

            expect(tipData.isValidating).toBe(true);
            expect(tipData.validationErrors).toEqual(['Error 1']);
        });
    });

    describe('Action permission methods', () => {
        it('should return true for canConfigure when rule is disabled and user has permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canConfigure(entity)).toBe(true);
        });

        it('should return false for canConfigure when rule is enabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canConfigure(entity)).toBe(false);
        });

        it('should return true for canEnable when rule is disabled, valid, and user has permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.status.validationStatus = 'VALID';
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canEnable(entity)).toBe(true);
        });

        it('should return false for canEnable when rule is invalid', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.status.validationStatus = 'INVALID';
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canEnable(entity)).toBe(false);
        });

        it('should return true for canDisable when rule is enabled and user has permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canDisable(entity)).toBe(true);
        });

        it('should return false for canDisable when rule is disabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canDisable(entity)).toBe(false);
        });

        it('should return true for canChangeVersion when rule is disabled, has permissions, and multiple versions available', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.permissions = { canRead: true, canWrite: true };
            entity.component.multipleVersionsAvailable = true;

            expect(component.canChangeVersion(entity)).toBe(true);
        });

        it('should return false for canChangeVersion when multiple versions not available', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.permissions = { canRead: true, canWrite: true };
            entity.component.multipleVersionsAvailable = false;

            expect(component.canChangeVersion(entity)).toBe(false);
        });

        it('should return true for canDelete when rule is disabled, user has permissions, and can modify parent', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.permissions = { canRead: true, canWrite: true };
            component.currentUser.controllerPermissions = { canRead: true, canWrite: true };

            expect(component.canDelete(entity)).toBe(true);
        });

        it('should return false for canDelete when rule is enabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';
            entity.permissions = { canRead: true, canWrite: true };
            component.currentUser.controllerPermissions = { canRead: true, canWrite: true };

            expect(component.canDelete(entity)).toBe(false);
        });

        it('should return true for canViewState when rule persists state and user has permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.persistsState = true;
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canViewState(entity)).toBe(true);
        });

        it('should return false for canViewState when rule does not persist state', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.component.persistsState = false;
            entity.permissions = { canRead: true, canWrite: true };

            expect(component.canViewState(entity)).toBe(false);
        });
    });

    describe('canClearBulletins', () => {
        it('should return true when user has write permissions and bulletins exist', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = true;
            entity.bulletins = [createTestBulletin()];

            expect(component.canClearBulletins(entity)).toBe(true);
        });

        it('should return false when user lacks write permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = false;
            entity.bulletins = [createTestBulletin()];

            expect(component.canClearBulletins(entity)).toBe(false);
        });

        it('should return false when bulletins array is empty', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = true;
            entity.bulletins = [];

            expect(component.canClearBulletins(entity)).toBe(false);
        });

        it('should return false when bulletins is undefined', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = true;
            entity.bulletins = undefined as any;

            expect(component.canClearBulletins(entity)).toBe(false);
        });

        it('should return false when user has write permissions but no bulletins', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = true;
            entity.bulletins = [];

            expect(component.canClearBulletins(entity)).toBe(false);
        });

        it('should return false when bulletins exist but user lacks write permissions', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.permissions.canWrite = false;
            entity.bulletins = [createTestBulletin(), createTestBulletin()];

            expect(component.canClearBulletins(entity)).toBe(false);
        });
    });

    describe('Formatting methods', () => {
        it('should format type correctly', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();

            const result = component.formatType(entity);

            expect(result).toBe('TestFlowAnalysisRule 1.0.0');
        });

        it('should format bundle correctly', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();

            const result = component.formatBundle(entity);

            expect(result).toBe('org.apache.nifi - test-bundle');
        });

        it('should format state as Disabled when rule is disabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.status.validationStatus = 'VALID';

            const result = component.formatState(entity);

            expect(result).toBe('Disabled');
        });

        it('should format state as Enabled when rule is enabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';
            entity.status.validationStatus = 'VALID';

            const result = component.formatState(entity);

            expect(result).toBe('Enabled');
        });

        it('should format state as Invalid when validation status is invalid', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.validationStatus = 'INVALID';

            const result = component.formatState(entity);

            expect(result).toBe('Invalid');
        });

        it('should format state as Validating when validation status is validating', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.validationStatus = 'VALIDATING';

            const result = component.formatState(entity);

            expect(result).toBe('Validating');
        });
    });

    describe('State icon methods', () => {
        it('should return validating icon when validation status is validating', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.validationStatus = 'VALIDATING';

            const result = component.getStateIcon(entity);

            expect(result).toBe('validating neutral-color fa fa-spin fa-circle-o-notch');
        });

        it('should return invalid icon when validation status is invalid', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.validationStatus = 'INVALID';

            const result = component.getStateIcon(entity);

            expect(result).toBe('invalid fa fa-warning caution-color');
        });

        it('should return disabled icon when rule is disabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'DISABLED';
            entity.status.validationStatus = 'VALID';

            const result = component.getStateIcon(entity);

            expect(result).toBe('disabled neutral-color icon icon-enable-false');
        });

        it('should return enabled icon when rule is enabled', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            entity.status.runStatus = 'ENABLED';
            entity.status.validationStatus = 'VALID';

            const result = component.getStateIcon(entity);

            expect(result).toBe('enabled success-color-variant fa fa-flash');
        });
    });

    describe('Selection methods', () => {
        it('should return true for isSelected when entity id matches selected id', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            component.selectedFlowAnalysisRuleId = entity.id;

            expect(component.isSelected(entity)).toBe(true);
        });

        it('should return false for isSelected when entity id does not match selected id', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            component.selectedFlowAnalysisRuleId = 'different-id';

            expect(component.isSelected(entity)).toBe(false);
        });

        it('should return false for isSelected when no rule is selected', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            component.selectedFlowAnalysisRuleId = '';

            expect(component.isSelected(entity)).toBe(false);
        });
    });

    describe('Sorting methods', () => {
        it('should sort flow analysis rules by name in ascending order', async () => {
            const { component } = await setup();
            const entity1 = createMockFlowAnalysisRuleEntity();
            entity1.component.name = 'B Rule';
            const entity2 = createMockFlowAnalysisRuleEntity();
            entity2.component.name = 'A Rule';

            const sort: Sort = { active: 'name', direction: 'asc' };
            const result = component.sortFlowAnalysisRules([entity1, entity2], sort);

            expect(result[0].component.name).toBe('A Rule');
            expect(result[1].component.name).toBe('B Rule');
        });

        it('should sort flow analysis rules by type in descending order', async () => {
            const { component } = await setup();
            const entity1 = createMockFlowAnalysisRuleEntity();
            entity1.component.type = 'org.apache.nifi.AType';
            const entity2 = createMockFlowAnalysisRuleEntity();
            entity2.component.type = 'org.apache.nifi.BType';

            const sort: Sort = { active: 'type', direction: 'desc' };
            const result = component.sortFlowAnalysisRules([entity1, entity2], sort);

            // The sorting is based on formatType() which formats the type name
            expect(result[0].component.type).toBe('org.apache.nifi.BType');
            expect(result[1].component.type).toBe('org.apache.nifi.AType');
        });

        it('should update sort and re-sort data when updateSort is called', async () => {
            const { component } = await setup();
            const entity1 = createMockFlowAnalysisRuleEntity();
            entity1.component.name = 'B Rule';
            const entity2 = createMockFlowAnalysisRuleEntity();
            entity2.component.name = 'A Rule';

            component.dataSource.data = [entity1, entity2];
            const sort: Sort = { active: 'name', direction: 'asc' };

            component.updateSort(sort);

            expect(component.sort).toEqual(sort);
            expect(component.dataSource.data[0].component.name).toBe('A Rule');
            expect(component.dataSource.data[1].component.name).toBe('B Rule');
        });
    });

    describe('Event emitters', () => {
        it('should emit selectFlowAnalysisRule when select is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.selectFlowAnalysisRule, 'next');

            component.select(entity);

            expect(component.selectFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });

        it('should emit viewFlowAnalysisRuleDocumentation when viewDocumentationClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.viewFlowAnalysisRuleDocumentation, 'next');

            component.viewDocumentationClicked(entity);

            expect(component.viewFlowAnalysisRuleDocumentation.next).toHaveBeenCalledWith(entity);
        });

        it('should emit configureFlowAnalysisRule when configureClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.configureFlowAnalysisRule, 'next');

            component.configureClicked(entity);

            expect(component.configureFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });

        it('should emit enableFlowAnalysisRule when enabledClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.enableFlowAnalysisRule, 'next');

            component.enabledClicked(entity);

            expect(component.enableFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });

        it('should emit disableFlowAnalysisRule when disableClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.disableFlowAnalysisRule, 'next');

            component.disableClicked(entity);

            expect(component.disableFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });

        it('should emit changeFlowAnalysisRuleVersion when changeVersionClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.changeFlowAnalysisRuleVersion, 'next');

            component.changeVersionClicked(entity);

            expect(component.changeFlowAnalysisRuleVersion.next).toHaveBeenCalledWith(entity);
        });

        it('should emit deleteFlowAnalysisRule when deleteClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.deleteFlowAnalysisRule, 'next');

            component.deleteClicked(entity);

            expect(component.deleteFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });

        it('should emit viewStateFlowAnalysisRule when viewStateClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.viewStateFlowAnalysisRule, 'next');

            component.viewStateClicked(entity);

            expect(component.viewStateFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });

        it('should emit clearBulletinsFlowAnalysisRule when clearBulletinsClicked is called', async () => {
            const { component } = await setup();
            const entity = createMockFlowAnalysisRuleEntity();
            jest.spyOn(component.clearBulletinsFlowAnalysisRule, 'next');

            component.clearBulletinsClicked(entity);

            expect(component.clearBulletinsFlowAnalysisRule.next).toHaveBeenCalledWith(entity);
        });
    });

    describe('Data input', () => {
        it('should update dataSource when flowAnalysisRules input is set', async () => {
            const { component } = await setup();
            const entities = [createMockFlowAnalysisRuleEntity(), createMockFlowAnalysisRuleEntity()];
            entities[1].id = 'different-id';

            component.flowAnalysisRules = entities;

            expect(component.dataSource.data).toEqual(entities);
        });

        it('should sort flowAnalysisRules when input is set', async () => {
            const { component } = await setup();
            const entity1 = createMockFlowAnalysisRuleEntity();
            entity1.component.name = 'B Rule';
            const entity2 = createMockFlowAnalysisRuleEntity();
            entity2.component.name = 'A Rule';
            entity2.id = 'different-id';

            component.sort = { active: 'name', direction: 'asc' };
            component.flowAnalysisRules = [entity1, entity2];

            expect(component.dataSource.data[0].component.name).toBe('A Rule');
            expect(component.dataSource.data[1].component.name).toBe('B Rule');
        });
    });
});
