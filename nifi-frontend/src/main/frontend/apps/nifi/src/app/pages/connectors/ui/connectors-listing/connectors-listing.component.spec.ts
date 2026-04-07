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
import { ConnectorsListing } from './connectors-listing.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { ComponentType, ConnectorAction, ConnectorActionName, ConnectorEntity } from '@nifi/shared';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import {
    cancelConnectorDrain,
    loadConnectorsListing,
    navigateToConfigureConnector,
    navigateToManageAccessPolicies,
    navigateToViewConnector,
    navigateToViewConnectorDetails,
    openNewConnectorDialog,
    openRenameConnectorDialog,
    promptConnectorDeletion,
    promptDiscardConnectorConfig,
    promptDrainConnector,
    selectConnector,
    startConnector,
    stopConnector
} from '../../state/connectors-listing/connectors-listing.actions';
import { promptPurgeConnector } from '../../state/purge-connector/purge-connector.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { loadExtensionTypesForConnectors } from '../../../../state/extension-types/extension-types.actions';
import { initialState } from '../../state/connectors-listing/connectors-listing.reducer';
import { connectorsFeatureKey, connectorsListingFeatureKey, ConnectorsListingState } from '../../state';
import { currentUserFeatureKey, CurrentUser } from '../../../../state/current-user';
import { flowConfigurationFeatureKey } from '../../../../state/flow-configuration';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';

describe('ConnectorsListing', () => {
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
            canRead?: boolean;
            canWrite?: boolean;
            managedProcessGroupId?: string;
            availableActions?: ConnectorAction[];
        } = {}
    ): ConnectorEntity {
        const defaultActions: ConnectorAction[] = options.availableActions ?? [
            createMockAction('START', true),
            createMockAction('STOP', true),
            createMockAction('CONFIGURE', true),
            createMockAction('DELETE', true)
        ];

        return {
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
                type: 'org.apache.nifi.connector.TestConnector',
                name: options.name || 'Test Connector',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-test-nar',
                    version: '1.0.0'
                },
                state: 'STOPPED',
                managedProcessGroupId: options.managedProcessGroupId ?? 'pg-root-default',
                availableActions: defaultActions
            },
            status: {
                runStatus: 'STOPPED'
            }
        };
    }

    function createMockCurrentUser(
        options: {
            canRead?: boolean;
            canWrite?: boolean;
        } = {}
    ): CurrentUser {
        return {
            identity: 'admin',
            anonymous: false,
            provenancePermissions: { canRead: true, canWrite: true },
            countersPermissions: { canRead: true, canWrite: true },
            tenantsPermissions: { canRead: true, canWrite: true },
            controllerPermissions: { canRead: true, canWrite: true },
            policiesPermissions: { canRead: true, canWrite: true },
            systemPermissions: { canRead: true, canWrite: true },
            parameterContextPermissions: { canRead: true, canWrite: true },
            restrictedComponentsPermissions: { canRead: true, canWrite: true },
            connectorsPermissions: {
                canRead: options.canRead ?? true,
                canWrite: options.canWrite ?? true
            },
            componentRestrictionPermissions: [],
            canVersionFlows: true,
            logoutSupported: true
        };
    }

    async function setup(
        options: {
            connectors?: ConnectorEntity[];
            currentUser?: CurrentUser;
            loadedTimestamp?: string;
            status?: 'pending' | 'loading' | 'success';
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [ConnectorsListing],
            providers: [
                provideMockStore({
                    initialState: {
                        [connectorsFeatureKey]: {
                            [connectorsListingFeatureKey]: {
                                connectors: options.connectors || [],
                                saving: false,
                                loadedTimestamp: options.loadedTimestamp || initialState.loadedTimestamp,
                                status: options.status || ('pending' as const)
                            }
                        },
                        [currentUserFeatureKey]: {
                            ...(fromCurrentUser.initialState || {}),
                            user: options.currentUser || createMockCurrentUser(),
                            status: 'success' as const
                        },
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState
                    }
                })
            ],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const fixture = TestBed.createComponent(ConnectorsListing);
        const component = fixture.componentInstance;

        return { component, fixture, store };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component, fixture } = await setup();
            fixture.detectChanges();
            expect(component).toBeTruthy();
        });

        it('should dispatch loadExtensionTypesForConnectors on init', async () => {
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(dispatchSpy).toHaveBeenCalledWith(loadExtensionTypesForConnectors());
        });

        it('should dispatch loadConnectorsListing on init', async () => {
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(dispatchSpy).toHaveBeenCalledWith(loadConnectorsListing());
        });
    });

    describe('Initial loading detection', () => {
        it('should return true for initial state', async () => {
            const { component } = await setup();
            const state: ConnectorsListingState = {
                connectors: [],
                saving: false,
                loadedTimestamp: initialState.loadedTimestamp,
                status: 'pending'
            };

            expect(component.isInitialLoading(state)).toBe(true);
        });

        it('should return false after data is loaded', async () => {
            const { component } = await setup();
            const state: ConnectorsListingState = {
                connectors: [createMockConnector()],
                saving: false,
                loadedTimestamp: '10/27/2025 12:00:00',
                status: 'success'
            };

            expect(component.isInitialLoading(state)).toBe(false);
        });
    });

    describe('Action dispatching', () => {
        it('should dispatch openNewConnectorDialog', async () => {
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.openNewConnectorDialog();

            expect(dispatchSpy).toHaveBeenCalledWith(openNewConnectorDialog());
        });

        it('should dispatch loadConnectorsListing when refreshing', async () => {
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.refreshConnectorListing();

            expect(dispatchSpy).toHaveBeenCalledWith(loadConnectorsListing());
        });

        it('should dispatch selectConnector with connector ID', async () => {
            const connector = createMockConnector({ id: 'connector-123' });
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.selectConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(selectConnector({ request: { id: 'connector-123' } }));
        });

        it('should dispatch navigateToViewConnector', async () => {
            const connector = createMockConnector({ id: 'connector-456', managedProcessGroupId: 'pg-root-123' });
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.viewConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToViewConnector({
                    request: {
                        connectorId: 'connector-456',
                        processGroupId: 'pg-root-123'
                    }
                })
            );
        });

        it('should dispatch navigateToViewConnectorDetails', async () => {
            const connector = createMockConnector({ id: 'connector-789' });
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.viewDetails(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(navigateToViewConnectorDetails({ id: 'connector-789' }));
        });

        it('should dispatch navigateToConfigureConnector', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.configureConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToConfigureConnector({
                    request: { id: connector.id, connector }
                })
            );
        });

        it('should dispatch openRenameConnectorDialog', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.renameConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(openRenameConnectorDialog({ connector }));
        });

        it('should dispatch startConnector', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.startConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(startConnector({ request: { connector } }));
        });

        it('should dispatch stopConnector', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.stopConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(stopConnector({ request: { connector } }));
        });

        it('should dispatch promptConnectorDeletion', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.deleteConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(promptConnectorDeletion({ request: { connector } }));
        });

        it('should dispatch promptDiscardConnectorConfig', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.discardConnectorConfig(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(promptDiscardConnectorConfig({ request: { connector } }));
        });

        it('should dispatch promptDrainConnector', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.drainConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(promptDrainConnector({ request: { connector } }));
        });

        it('should dispatch cancelConnectorDrain', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.cancelDrainConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(cancelConnectorDrain({ request: { connector } }));
        });

        it('should dispatch promptPurgeConnector', async () => {
            const connector = createMockConnector();
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.purgeConnector(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(promptPurgeConnector({ request: { connector } }));
        });

        it('should dispatch navigateToManageAccessPolicies', async () => {
            const connector = createMockConnector({ id: 'connector-policy' });
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.navigateToManageAccessPolicies(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(navigateToManageAccessPolicies({ id: 'connector-policy' }));
        });

        it('should dispatch navigateToComponentDocumentation', async () => {
            const connector = createMockConnector({ id: 'connector-doc-1' });
            const { component, store } = await setup();
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            component.viewDocumentation(connector);

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToComponentDocumentation({
                    request: {
                        backNavigation: {
                            route: ['/connectors', 'connector-doc-1'],
                            routeBoundary: ['/documentation'],
                            context: 'connector'
                        },
                        parameters: {
                            componentType: ComponentType.Connector,
                            type: connector.component.type,
                            group: connector.component.bundle.group,
                            artifact: connector.component.bundle.artifact,
                            version: connector.component.bundle.version
                        }
                    }
                })
            );
        });
    });
});
