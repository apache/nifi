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
import { ConnectorTable } from './connector-table.component';
import { ConnectorAction, ConnectorActionName, ConnectorEntity, ConnectorStatus, NiFiCommon } from '@nifi/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { By } from '@angular/platform-browser';
import { FlowConfiguration } from '../../../../state/flow-configuration';
import { CurrentUser } from '../../../../state/current-user';

describe('ConnectorTable', () => {
    function createMockAction(name: ConnectorActionName, allowed = true, reasonNotAllowed?: string): ConnectorAction {
        const action: ConnectorAction = {
            name,
            description: `${name} action`,
            allowed
        };
        if (reasonNotAllowed !== undefined) {
            action.reasonNotAllowed = reasonNotAllowed;
        }
        return action;
    }

    function createMockConnector(
        options: {
            id?: string;
            name?: string;
            type?: string;
            state?: string;
            canRead?: boolean;
            canWrite?: boolean;
            canOperate?: boolean;
            managedProcessGroupId?: string;
            validationErrors?: string[];
            validationStatus?: string;
            availableActions?: ConnectorAction[];
        } = {}
    ): ConnectorEntity {
        const defaultActions: ConnectorAction[] = options.availableActions ?? [
            createMockAction('START', true),
            createMockAction('STOP', true),
            createMockAction('CONFIGURE', true),
            createMockAction('DELETE', true)
        ];

        const connector: ConnectorEntity = {
            id: options.id || 'connector-123',
            uri: `http://localhost/nifi-api/connectors/${options.id || 'connector-123'}`,
            permissions: {
                canRead: options.canRead ?? true,
                canWrite: options.canWrite ?? true
            },
            revision: { version: 1, clientId: 'client-1' },
            bulletins: [],
            component: {
                id: options.id || 'connector-123',
                type: options.type || 'org.apache.nifi.connector.TestConnector',
                name: options.name || 'Test Connector',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-test-nar',
                    version: '1.0.0'
                },
                state: options.state || 'STOPPED',
                managedProcessGroupId: options.managedProcessGroupId ?? 'pg-root-default',
                validationErrors: options.validationErrors,
                validationStatus: options.validationStatus,
                availableActions: defaultActions
            },
            status: {
                runStatus: options.state || 'STOPPED'
            } as ConnectorStatus
        };

        if (options.canOperate !== undefined) {
            connector.operatePermissions = {
                canRead: true,
                canWrite: options.canOperate
            };
        }

        return connector;
    }

    function createMockConnectors(): ConnectorEntity[] {
        return [
            createMockConnector({ id: 'connector-1', name: 'Alpha Connector' }),
            createMockConnector({ id: 'connector-2', name: 'Beta Connector' }),
            createMockConnector({ id: 'connector-3', name: 'Charlie Connector' })
        ];
    }

    function createMockFlowConfiguration(
        options: {
            supportsManagedAuthorizer?: boolean;
        } = {}
    ): FlowConfiguration {
        return {
            supportsManagedAuthorizer: options.supportsManagedAuthorizer ?? true,
            supportsConfigurableAuthorizer: true,
            supportsConfigurableUsersAndGroups: true,
            currentTime: '2025-01-01T00:00:00.000Z',
            timeOffset: 0,
            defaultBackPressureDataSizeThreshold: 1073741824,
            defaultBackPressureObjectThreshold: 10000
        };
    }

    function createMockCurrentUser(
        options: {
            canReadTenants?: boolean;
        } = {}
    ): CurrentUser {
        return {
            identity: 'test-user',
            anonymous: false,
            logoutSupported: true,
            canVersionFlows: false,
            provenancePermissions: { canRead: true, canWrite: false },
            countersPermissions: { canRead: true, canWrite: false },
            tenantsPermissions: { canRead: options.canReadTenants ?? true, canWrite: false },
            controllerPermissions: { canRead: true, canWrite: false },
            policiesPermissions: { canRead: true, canWrite: false },
            systemPermissions: { canRead: true, canWrite: false },
            parameterContextPermissions: { canRead: true, canWrite: false },
            restrictedComponentsPermissions: { canRead: true, canWrite: false },
            connectorsPermissions: { canRead: true, canWrite: true },
            componentRestrictionPermissions: []
        };
    }

    interface SetupOptions {
        connectors?: ConnectorEntity[];
        selectedConnectorId?: string;
        initialSortColumn?: 'name' | 'type' | 'bundle' | 'state';
        initialSortDirection?: 'asc' | 'desc';
        flowConfiguration?: FlowConfiguration;
        currentUser?: CurrentUser;
    }

    async function setup(options: SetupOptions = {}) {
        const mockNiFiCommon = {
            formatType: vi.fn().mockReturnValue('TestConnector'),
            formatBundle: vi.fn().mockReturnValue('nifi-test-nar - 1.0.0'),
            compareString: vi.fn((a: string, b: string) => a.localeCompare(b)),
            isEmpty: vi.fn((arr: unknown) => !arr || (Array.isArray(arr) && arr.length === 0))
        };

        await TestBed.configureTestingModule({
            imports: [ConnectorTable, NoopAnimationsModule],
            providers: [{ provide: NiFiCommon, useValue: mockNiFiCommon }]
        }).compileComponents();

        const fixture = TestBed.createComponent(ConnectorTable);
        const component = fixture.componentInstance;

        if (options.initialSortColumn) {
            component.initialSortColumn = options.initialSortColumn;
        }
        if (options.initialSortDirection) {
            component.initialSortDirection = options.initialSortDirection;
        }
        if (options.selectedConnectorId !== undefined) {
            component.selectedConnectorId = options.selectedConnectorId;
        }
        if (options.connectors) {
            component.connectors = options.connectors;
        }
        if (options.flowConfiguration) {
            component.flowConfiguration = options.flowConfiguration;
        }
        if (options.currentUser) {
            component.currentUser = options.currentUser;
        }

        fixture.detectChanges();

        return { component, fixture, mockNiFiCommon };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should initialize with default sort column and direction', async () => {
            const { component } = await setup();
            expect(component.initialSortColumn).toBe('name');
            expect(component.initialSortDirection).toBe('asc');
        });

        it('should have correct displayed columns', async () => {
            const { component } = await setup();
            expect(component.displayedColumns).toEqual(['moreDetails', 'name', 'type', 'bundle', 'state', 'actions']);
        });
    });

    describe('Validation errors', () => {
        it('should return true for hasErrors when connector has validation errors', async () => {
            const { component } = await setup();
            const connector = createMockConnector({ validationErrors: ['Property X is required'] });
            expect(component.hasErrors(connector)).toBe(true);
        });

        it('should return false for hasErrors when connector has no validation errors', async () => {
            const { component } = await setup();
            const connector = createMockConnector({ validationErrors: [] });
            expect(component.hasErrors(connector)).toBe(false);
        });

        it('should return false for hasErrors when validationErrors is undefined', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            expect(component.hasErrors(connector)).toBe(false);
        });

        it('should return isValidating true when status is VALIDATING', async () => {
            const { component } = await setup();
            const connector = createMockConnector({
                validationErrors: ['Error 1'],
                validationStatus: 'VALIDATING'
            });
            const tipData = component.getValidationErrorsTipData(connector);
            expect(tipData.isValidating).toBe(true);
            expect(tipData.validationErrors).toEqual(['Error 1']);
        });

        it('should return isValidating false when status is not VALIDATING', async () => {
            const { component } = await setup();
            const connector = createMockConnector({
                validationErrors: ['Error 1', 'Error 2'],
                validationStatus: 'INVALID'
            });
            const tipData = component.getValidationErrorsTipData(connector);
            expect(tipData.isValidating).toBe(false);
            expect(tipData.validationErrors).toEqual(['Error 1', 'Error 2']);
        });

        it('should return empty array for validationErrors when undefined', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            const tipData = component.getValidationErrorsTipData(connector);
            expect(tipData.validationErrors).toEqual([]);
        });
    });

    describe('Permission checks', () => {
        it('should check canRead permission', async () => {
            const { component } = await setup();
            expect(component.canRead(createMockConnector({ canRead: true }))).toBe(true);
            expect(component.canRead(createMockConnector({ canRead: false }))).toBe(false);
        });

        it('should check canModify permission', async () => {
            const { component } = await setup();
            expect(component.canModify(createMockConnector({ canWrite: true }))).toBe(true);
            expect(component.canModify(createMockConnector({ canWrite: false }))).toBe(false);
        });

        it('should check canOperate with write permission', async () => {
            const { component } = await setup();
            expect(component.canOperate(createMockConnector({ canWrite: true }))).toBe(true);
        });

        it('should check canOperate with operate permission', async () => {
            const { component } = await setup();
            expect(component.canOperate(createMockConnector({ canWrite: false, canOperate: true }))).toBe(true);
        });

        it('should check canOperate with neither permission', async () => {
            const { component } = await setup();
            expect(component.canOperate(createMockConnector({ canWrite: false, canOperate: false }))).toBe(false);
        });

        it('should check canOperate without operatePermissions', async () => {
            const { component } = await setup();
            expect(component.canOperate(createMockConnector({ canWrite: false }))).toBe(false);
        });

        it('should check canConfigure', async () => {
            const { component } = await setup();
            expect(
                component.canConfigure(createMockConnector({ availableActions: [createMockAction('CONFIGURE', true)] }))
            ).toBe(true);
            expect(
                component.canConfigure(
                    createMockConnector({ availableActions: [createMockAction('CONFIGURE', false)] })
                )
            ).toBe(false);
            expect(
                component.canConfigure(createMockConnector({ availableActions: [createMockAction('START', true)] }))
            ).toBe(false);
        });

        it('should check canDelete', async () => {
            const { component } = await setup();
            expect(
                component.canDelete(createMockConnector({ availableActions: [createMockAction('DELETE', true)] }))
            ).toBe(true);
            expect(
                component.canDelete(createMockConnector({ availableActions: [createMockAction('DELETE', false)] }))
            ).toBe(false);
            expect(
                component.canDelete(createMockConnector({ availableActions: [createMockAction('START', true)] }))
            ).toBe(false);
        });

        it('should check canStart', async () => {
            const { component } = await setup();
            expect(
                component.canStart(createMockConnector({ availableActions: [createMockAction('START', true)] }))
            ).toBe(true);
            expect(
                component.canStart(createMockConnector({ availableActions: [createMockAction('START', false)] }))
            ).toBe(false);
        });

        it('should check canStop', async () => {
            const { component } = await setup();
            expect(component.canStop(createMockConnector({ availableActions: [createMockAction('STOP', true)] }))).toBe(
                true
            );
            expect(
                component.canStop(createMockConnector({ availableActions: [createMockAction('STOP', false)] }))
            ).toBe(false);
        });

        it('should check canDiscardConfig', async () => {
            const { component } = await setup();
            expect(
                component.canDiscardConfig(
                    createMockConnector({
                        availableActions: [createMockAction('DISCARD_WORKING_CONFIGURATION', true)]
                    })
                )
            ).toBe(true);
            expect(
                component.canDiscardConfig(
                    createMockConnector({
                        availableActions: [createMockAction('DISCARD_WORKING_CONFIGURATION', false)]
                    })
                )
            ).toBe(false);
        });

        it('should check canDrain', async () => {
            const { component } = await setup();
            expect(
                component.canDrain(
                    createMockConnector({ availableActions: [createMockAction('DRAIN_FLOWFILES', true)] })
                )
            ).toBe(true);
            expect(
                component.canDrain(
                    createMockConnector({ availableActions: [createMockAction('DRAIN_FLOWFILES', false)] })
                )
            ).toBe(false);
        });

        it('should check canCancelDrain', async () => {
            const { component } = await setup();
            expect(
                component.canCancelDrain(
                    createMockConnector({ availableActions: [createMockAction('CANCEL_DRAIN_FLOWFILES', true)] })
                )
            ).toBe(true);
            expect(
                component.canCancelDrain(
                    createMockConnector({ availableActions: [createMockAction('CANCEL_DRAIN_FLOWFILES', false)] })
                )
            ).toBe(false);
        });

        it('should check canPurge', async () => {
            const { component } = await setup();
            expect(
                component.canPurge(
                    createMockConnector({ availableActions: [createMockAction('PURGE_FLOWFILES', true)] })
                )
            ).toBe(true);
            expect(
                component.canPurge(
                    createMockConnector({ availableActions: [createMockAction('PURGE_FLOWFILES', false)] })
                )
            ).toBe(false);
        });
    });

    describe('Data formatting', () => {
        it('should format name when readable', async () => {
            const { component } = await setup();
            expect(component.formatName(createMockConnector({ name: 'My Connector', canRead: true }))).toBe(
                'My Connector'
            );
        });

        it('should return ID when not readable', async () => {
            const { component } = await setup();
            expect(component.formatName(createMockConnector({ id: 'connector-xyz', canRead: false }))).toBe(
                'connector-xyz'
            );
        });

        it('should format type when readable', async () => {
            const { component, mockNiFiCommon } = await setup();
            const connector = createMockConnector({ canRead: true });
            component.formatType(connector);
            expect(mockNiFiCommon.formatType).toHaveBeenCalledWith(connector.component);
        });

        it('should return empty string for type when not readable', async () => {
            const { component } = await setup();
            expect(component.formatType(createMockConnector({ canRead: false }))).toBe('');
        });

        it('should format bundle when readable', async () => {
            const { component, mockNiFiCommon } = await setup();
            const connector = createMockConnector({ canRead: true });
            component.formatBundle(connector);
            expect(mockNiFiCommon.formatBundle).toHaveBeenCalledWith(connector.component.bundle);
        });

        it('should format state when readable', async () => {
            const { component } = await setup();
            expect(component.formatState(createMockConnector({ state: 'RUNNING', canRead: true }))).toBe('RUNNING');
        });

        it('should return empty string for state when not readable', async () => {
            const { component } = await setup();
            expect(component.formatState(createMockConnector({ canRead: false }))).toBe('');
        });
    });

    describe('Selection', () => {
        it('should detect selected connector', async () => {
            const { component } = await setup({ selectedConnectorId: 'connector-123' });
            expect(component.isSelected(createMockConnector({ id: 'connector-123' }))).toBe(true);
        });

        it('should detect non-selected connector', async () => {
            const { component } = await setup({ selectedConnectorId: 'other-id' });
            expect(component.isSelected(createMockConnector({ id: 'connector-123' }))).toBe(false);
        });

        it('should emit selectConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.selectConnector, 'next');

            component.select(connector);

            expect(component.selectConnector.next).toHaveBeenCalledWith(connector);
        });
    });

    describe('Output events', () => {
        it('should emit viewConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.viewConnector, 'next');
            component.viewClicked(connector);
            expect(component.viewConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit viewDetails event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.viewDetails, 'next');
            component.viewDetailsClicked(connector);
            expect(component.viewDetails.next).toHaveBeenCalledWith(connector);
        });

        it('should emit configureConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.configureConnector, 'next');
            component.configureClicked(connector);
            expect(component.configureConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit renameConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.renameConnector, 'next');
            component.renameClicked(connector);
            expect(component.renameConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit startConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.startConnector, 'next');
            component.startClicked(connector);
            expect(component.startConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit stopConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.stopConnector, 'next');
            component.stopClicked(connector);
            expect(component.stopConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit deleteConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.deleteConnector, 'next');
            component.deleteClicked(connector);
            expect(component.deleteConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit discardConnectorConfig event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.discardConnectorConfig, 'next');
            component.discardConfigClicked(connector);
            expect(component.discardConnectorConfig.next).toHaveBeenCalledWith(connector);
        });

        it('should emit drainConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.drainConnector, 'next');
            component.drainClicked(connector);
            expect(component.drainConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit cancelDrainConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.cancelDrainConnector, 'next');
            component.cancelDrainClicked(connector);
            expect(component.cancelDrainConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit purgeConnector event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.purgeConnector, 'next');
            component.purgeClicked(connector);
            expect(component.purgeConnector.next).toHaveBeenCalledWith(connector);
        });

        it('should emit manageAccessPolicies event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.manageAccessPolicies, 'next');
            component.manageAccessPoliciesClicked(connector);
            expect(component.manageAccessPolicies.next).toHaveBeenCalledWith(connector);
        });

        it('should emit viewDocumentation event', async () => {
            const { component } = await setup();
            const connector = createMockConnector();
            vi.spyOn(component.viewDocumentation, 'next');
            component.viewDocumentationClicked(connector);
            expect(component.viewDocumentation.next).toHaveBeenCalledWith(connector);
        });
    });

    describe('Access policies permissions', () => {
        it('should allow when managed authorizer and can read tenants', async () => {
            const { component } = await setup({
                flowConfiguration: createMockFlowConfiguration({ supportsManagedAuthorizer: true }),
                currentUser: createMockCurrentUser({ canReadTenants: true })
            });
            expect(component.canManageAccessPolicies()).toBe(true);
        });

        it('should deny when managed authorizer not supported', async () => {
            const { component } = await setup({
                flowConfiguration: createMockFlowConfiguration({ supportsManagedAuthorizer: false }),
                currentUser: createMockCurrentUser({ canReadTenants: true })
            });
            expect(component.canManageAccessPolicies()).toBe(false);
        });

        it('should deny when cannot read tenants', async () => {
            const { component } = await setup({
                flowConfiguration: createMockFlowConfiguration({ supportsManagedAuthorizer: true }),
                currentUser: createMockCurrentUser({ canReadTenants: false })
            });
            expect(component.canManageAccessPolicies()).toBe(false);
        });

        it('should be falsy when flowConfiguration is undefined', async () => {
            const { component } = await setup({
                currentUser: createMockCurrentUser({ canReadTenants: true })
            });
            expect(component.canManageAccessPolicies()).toBeFalsy();
        });
    });

    describe('Sorting', () => {
        it('should sort by name ascending', async () => {
            const connectors = createMockConnectors();
            const { component } = await setup({ connectors });

            component.sortData({ active: 'name', direction: 'asc' });

            expect(component.dataSource.data[0].component.name).toBe('Alpha Connector');
            expect(component.dataSource.data[2].component.name).toBe('Charlie Connector');
        });

        it('should sort by name descending', async () => {
            const connectors = createMockConnectors();
            const { component } = await setup({ connectors });

            component.sortData({ active: 'name', direction: 'desc' });

            expect(component.dataSource.data[0].component.name).toBe('Charlie Connector');
            expect(component.dataSource.data[2].component.name).toBe('Alpha Connector');
        });

        it('should update activeSort when sorting', async () => {
            const { component } = await setup();
            const sort = { active: 'name', direction: 'desc' as const };

            component.sortData(sort);

            expect(component.activeSort).toEqual(sort);
        });

        it('should handle empty connectors array', async () => {
            const { component } = await setup({ connectors: [] });
            expect(component.dataSource.data).toEqual([]);
        });
    });

    describe('Table rendering', () => {
        it('should render connector rows', async () => {
            const connectors = createMockConnectors();
            const { fixture } = await setup({ connectors });

            const rows = fixture.debugElement.queryAll(By.css('[data-qa="connector-table-row"]'));
            expect(rows.length).toBe(3);
        });

        it('should render validation errors icon when connector has errors', async () => {
            const connectors = [
                createMockConnector({ id: 'connector-1', validationErrors: ['Property X is required'] })
            ];
            const { fixture } = await setup({ connectors });

            const errorIcon = fixture.debugElement.query(By.css('[data-qa="connector-validation-errors"]'));
            expect(errorIcon).toBeTruthy();
        });

        it('should not render validation errors icon when connector has no errors', async () => {
            const connectors = [createMockConnector({ id: 'connector-1', validationErrors: [] })];
            const { fixture } = await setup({ connectors });

            const errorIcon = fixture.debugElement.query(By.css('[data-qa="connector-validation-errors"]'));
            expect(errorIcon).toBeFalsy();
        });

        it('should not render validation errors icon when user cannot read', async () => {
            const connectors = [
                createMockConnector({ id: 'connector-1', canRead: false, validationErrors: ['Error'] })
            ];
            const { fixture } = await setup({ connectors });

            const errorIcon = fixture.debugElement.query(By.css('[data-qa="connector-validation-errors"]'));
            expect(errorIcon).toBeFalsy();
        });
    });

    describe('Edits not applied badge', () => {
        it('should render when DISCARD_WORKING_CONFIGURATION is allowed', async () => {
            const connectors = [
                createMockConnector({
                    id: 'connector-1',
                    availableActions: [
                        createMockAction('START', true),
                        createMockAction('DISCARD_WORKING_CONFIGURATION', true)
                    ]
                })
            ];
            const { fixture } = await setup({ connectors });

            const badge = fixture.debugElement.query(By.css('[data-qa="edits-not-applied-badge"]'));
            expect(badge).toBeTruthy();
        });

        it('should not render when DISCARD_WORKING_CONFIGURATION is not allowed', async () => {
            const connectors = [
                createMockConnector({
                    id: 'connector-1',
                    availableActions: [
                        createMockAction('START', true),
                        createMockAction('DISCARD_WORKING_CONFIGURATION', false)
                    ]
                })
            ];
            const { fixture } = await setup({ connectors });

            const badge = fixture.debugElement.query(By.css('[data-qa="edits-not-applied-badge"]'));
            expect(badge).toBeFalsy();
        });

        it('should not render when DISCARD_WORKING_CONFIGURATION is not present', async () => {
            const connectors = [
                createMockConnector({
                    id: 'connector-1',
                    availableActions: [createMockAction('START', true), createMockAction('STOP', true)]
                })
            ];
            const { fixture } = await setup({ connectors });

            const badge = fixture.debugElement.query(By.css('[data-qa="edits-not-applied-badge"]'));
            expect(badge).toBeFalsy();
        });

        it('should not render when user cannot read', async () => {
            const connectors = [
                createMockConnector({
                    id: 'connector-1',
                    canRead: false,
                    availableActions: [createMockAction('DISCARD_WORKING_CONFIGURATION', true)]
                })
            ];
            const { fixture } = await setup({ connectors });

            const badge = fixture.debugElement.query(By.css('[data-qa="edits-not-applied-badge"]'));
            expect(badge).toBeFalsy();
        });
    });

    describe('Action availability helpers', () => {
        it('should return true when action is allowed', async () => {
            const { component } = await setup();
            const connector = createMockConnector({ availableActions: [createMockAction('START', true)] });
            expect(component.isActionAllowed(connector, 'START')).toBe(true);
        });

        it('should return false when action is not allowed', async () => {
            const { component } = await setup();
            const connector = createMockConnector({
                availableActions: [createMockAction('START', false, 'Some reason')]
            });
            expect(component.isActionAllowed(connector, 'START')).toBe(false);
        });

        it('should return disabled reason', async () => {
            const { component } = await setup();
            const connector = createMockConnector({
                availableActions: [createMockAction('DELETE', false, 'Connector must be stopped first')]
            });
            expect(component.getActionDisabledReason(connector, 'DELETE')).toBe('Connector must be stopped first');
        });

        it('should return empty string when action is allowed', async () => {
            const { component } = await setup();
            const connector = createMockConnector({ availableActions: [createMockAction('DELETE', true)] });
            expect(component.getActionDisabledReason(connector, 'DELETE')).toBe('');
        });
    });
});
