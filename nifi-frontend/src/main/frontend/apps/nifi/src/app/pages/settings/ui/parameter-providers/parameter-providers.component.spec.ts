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
import { ParameterProviders } from './parameter-providers.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { initialParameterProvidersState } from '../../state/parameter-providers/parameter-providers.reducer';
import { ParameterProviderEntity, parameterProvidersFeatureKey } from '../../state/parameter-providers';
import { settingsFeatureKey } from '../../state';
import { ComponentType } from '@nifi/shared';
import * as ParameterProviderActions from '../../state/parameter-providers/parameter-providers.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';

describe('ParameterProviders', () => {
    // Mock data factories
    const mockTimestampIso = '2023-10-08T12:00:00.000Z';

    function createMockParameterProviderEntity(): ParameterProviderEntity {
        return {
            id: 'test-parameter-provider-id',
            uri: 'test-uri',
            bulletins: [
                {
                    id: 1,
                    sourceId: 'test-parameter-provider-id',
                    groupId: 'test-group-id',
                    timestamp: '12:00:00 UTC',
                    timestampIso: mockTimestampIso,
                    canRead: true,
                    bulletin: {
                        id: 1,
                        category: 'INFO',
                        level: 'INFO',
                        message: 'Test bulletin',
                        timestamp: '12:00:00 UTC',
                        timestampIso: mockTimestampIso,
                        sourceId: 'test-parameter-provider-id',
                        groupId: 'test-group-id',
                        sourceType: 'PARAMETER_PROVIDER',
                        sourceName: 'Test Parameter Provider'
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
                validationStatus: 'VALID'
            }
        } as ParameterProviderEntity;
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [ParameterProviders],
            providers: [
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [parameterProvidersFeatureKey]: {
                                ...initialParameterProvidersState
                            }
                        }
                    }
                })
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(ParameterProviders);
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
        it('should dispatch loadParameterProviders on ngOnInit', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(store.dispatch).toHaveBeenCalledWith(ParameterProviderActions.loadParameterProviders());
        });

        it('should dispatch resetParameterProvidersState on ngOnDestroy', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnDestroy();

            expect(store.dispatch).toHaveBeenCalledWith(ParameterProviderActions.resetParameterProvidersState());
        });

        it('should return true for isInitialLoading when timestamp matches initial state', async () => {
            const { component } = await setup();
            const state = { ...initialParameterProvidersState };
            expect(component.isInitialLoading(state)).toBe(true);
        });

        it('should return false for isInitialLoading when timestamp differs from initial state', async () => {
            const { component } = await setup();
            const state = {
                ...initialParameterProvidersState,
                loadedTimestamp: '2023-01-01 12:00:00 EST'
            };
            expect(component.isInitialLoading(state)).toBe(false);
        });
    });

    describe('Action dispatching', () => {
        it('should dispatch loadParameterProviders action when refreshParameterProvidersListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.refreshParameterProvidersListing();

            expect(store.dispatch).toHaveBeenCalledWith(ParameterProviderActions.loadParameterProviders());
        });

        it('should dispatch openNewParameterProviderDialog action when openNewParameterProviderDialog is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.openNewParameterProviderDialog();

            expect(store.dispatch).toHaveBeenCalledWith(ParameterProviderActions.openNewParameterProviderDialog());
        });

        it('should dispatch navigateToEditParameterProvider action when openConfigureParameterProviderDialog is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.openConfigureParameterProviderDialog(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.navigateToEditParameterProvider({
                    id: mockParameterProviderEntity.component.id
                })
            );
        });

        it('should dispatch selectParameterProvider action when selectParameterProvider is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.selectParameterProvider(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.selectParameterProvider({
                    request: {
                        id: mockParameterProviderEntity.id
                    }
                })
            );
        });

        it('should dispatch navigateToAdvancedParameterProviderUi action when openAdvancedUi is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.openAdvancedUi(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.navigateToAdvancedParameterProviderUi({
                    id: mockParameterProviderEntity.id
                })
            );
        });

        it('should dispatch navigateToManageAccessPolicies action when navigateToManageAccessPolicies is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.navigateToManageAccessPolicies(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.navigateToManageAccessPolicies({
                    id: mockParameterProviderEntity.id
                })
            );
        });

        it('should dispatch promptParameterProviderDeletion action when deleteParameterProvider is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.deleteParameterProvider(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.promptParameterProviderDeletion({
                    request: {
                        parameterProvider: mockParameterProviderEntity
                    }
                })
            );
        });

        it('should dispatch navigateToFetchParameterProvider action when fetchParameterProviderParameters is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.fetchParameterProviderParameters(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.navigateToFetchParameterProvider({
                    id: mockParameterProviderEntity.component.id
                })
            );
        });

        it('should dispatch clearParameterProviderBulletins action when clearBulletinsParameterProvider is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.clearBulletinsParameterProvider(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                ParameterProviderActions.clearParameterProviderBulletins({
                    request: {
                        uri: mockParameterProviderEntity.uri,
                        fromTimestamp: mockTimestampIso,
                        componentId: mockParameterProviderEntity.id,
                        componentType: ComponentType.ParameterProvider
                    }
                })
            );
        });

        it('should dispatch navigateToComponentDocumentation action when viewParameterProviderDocumentation is called', async () => {
            const { component, store } = await setup();
            const mockParameterProviderEntity = createMockParameterProviderEntity();
            jest.spyOn(store, 'dispatch');

            component.viewParameterProviderDocumentation(mockParameterProviderEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                navigateToComponentDocumentation({
                    request: {
                        backNavigation: {
                            route: ['/settings', 'parameter-providers', mockParameterProviderEntity.id],
                            routeBoundary: ['/documentation'],
                            context: 'Parameter Provider'
                        },
                        parameters: {
                            componentType: ComponentType.ParameterProvider,
                            type: mockParameterProviderEntity.component.type,
                            group: mockParameterProviderEntity.component.bundle.group,
                            artifact: mockParameterProviderEntity.component.bundle.artifact,
                            version: mockParameterProviderEntity.component.bundle.version
                        }
                    }
                })
            );
        });
    });
});
