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
import { RegistryClients } from './registry-clients.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/registry-clients/registry-clients.reducer';
import { registryClientsFeatureKey } from '../../state/registry-clients';
import { settingsFeatureKey } from '../../state';
import { RegistryClientEntity } from '../../../../state/shared';
import { ComponentType } from '@nifi/shared';
import * as RegistryClientsActions from '../../state/registry-clients/registry-clients.actions';
import { currentUserFeatureKey } from '../../../../state/current-user';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';

describe('RegistryClients', () => {
    // Mock data factories
    function createMockRegistryClientEntity(): RegistryClientEntity {
        return {
            id: 'test-registry-client-id',
            uri: 'test-uri',
            bulletins: [
                {
                    id: 1,
                    sourceId: 'test-registry-client-id',
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
                        sourceId: 'test-registry-client-id',
                        groupId: 'test-group-id',
                        sourceType: 'REGISTRY_CLIENT',
                        sourceName: 'Test Registry Client'
                    }
                }
            ],
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
            }
        };
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [RegistryClients],
            providers: [
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [registryClientsFeatureKey]: {
                                ...initialState
                            }
                        },
                        [currentUserFeatureKey]: fromCurrentUser.initialState
                    }
                })
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(RegistryClients);
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
        it('should dispatch loadRegistryClients on ngOnInit', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(store.dispatch).toHaveBeenCalledWith(RegistryClientsActions.loadRegistryClients());
        });

        it('should dispatch resetRegistryClientsState on ngOnDestroy', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnDestroy();

            expect(store.dispatch).toHaveBeenCalledWith(RegistryClientsActions.resetRegistryClientsState());
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
        it('should dispatch loadRegistryClients action when refreshRegistryClientListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.refreshRegistryClientListing();

            expect(store.dispatch).toHaveBeenCalledWith(RegistryClientsActions.loadRegistryClients());
        });

        it('should dispatch openNewRegistryClientDialog action when openNewRegistryClientDialog is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.openNewRegistryClientDialog();

            expect(store.dispatch).toHaveBeenCalledWith(RegistryClientsActions.openNewRegistryClientDialog());
        });

        it('should dispatch selectClient action when selectRegistryClient is called', async () => {
            const { component, store } = await setup();
            const mockRegistryClientEntity = createMockRegistryClientEntity();
            jest.spyOn(store, 'dispatch');

            component.selectRegistryClient(mockRegistryClientEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                RegistryClientsActions.selectClient({
                    request: {
                        id: mockRegistryClientEntity.id
                    }
                })
            );
        });

        it('should dispatch navigateToEditRegistryClient action when configureRegistryClient is called', async () => {
            const { component, store } = await setup();
            const mockRegistryClientEntity = createMockRegistryClientEntity();
            jest.spyOn(store, 'dispatch');

            component.configureRegistryClient(mockRegistryClientEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                RegistryClientsActions.navigateToEditRegistryClient({
                    id: mockRegistryClientEntity.id
                })
            );
        });

        it('should dispatch promptRegistryClientDeletion action when deleteRegistryClient is called', async () => {
            const { component, store } = await setup();
            const mockRegistryClientEntity = createMockRegistryClientEntity();
            jest.spyOn(store, 'dispatch');

            component.deleteRegistryClient(mockRegistryClientEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                RegistryClientsActions.promptRegistryClientDeletion({
                    request: {
                        registryClient: mockRegistryClientEntity
                    }
                })
            );
        });

        it('should dispatch clearRegistryClientBulletins action when clearBulletinsRegistryClient is called', async () => {
            const { component, store } = await setup();
            const mockRegistryClientEntity = createMockRegistryClientEntity();
            jest.spyOn(store, 'dispatch');

            component.clearBulletinsRegistryClient(mockRegistryClientEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                RegistryClientsActions.clearRegistryClientBulletins({
                    request: {
                        uri: mockRegistryClientEntity.uri,
                        fromTimestamp: '2023-10-08T12:00:00.000Z',
                        componentId: mockRegistryClientEntity.id,
                        componentType: ComponentType.FlowRegistryClient
                    }
                })
            );
        });

        it('should dispatch navigateToComponentDocumentation action when viewRegistryClientDocumentation is called', async () => {
            const { component, store } = await setup();
            const mockRegistryClientEntity = createMockRegistryClientEntity();
            jest.spyOn(store, 'dispatch');

            component.viewRegistryClientDocumentation(mockRegistryClientEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                navigateToComponentDocumentation({
                    request: {
                        backNavigation: {
                            route: ['/settings', 'registry-clients', mockRegistryClientEntity.id],
                            routeBoundary: ['/documentation'],
                            context: 'Registry Client'
                        },
                        parameters: {
                            componentType: ComponentType.FlowRegistryClient,
                            type: mockRegistryClientEntity.component.type,
                            group: mockRegistryClientEntity.component.bundle.group,
                            artifact: mockRegistryClientEntity.component.bundle.artifact,
                            version: mockRegistryClientEntity.component.bundle.version
                        }
                    }
                })
            );
        });
    });
});
