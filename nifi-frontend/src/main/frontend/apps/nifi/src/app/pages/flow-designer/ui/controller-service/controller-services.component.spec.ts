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
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ControllerServices } from './controller-services.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/controller-services/controller-services.reducer';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { canvasFeatureKey } from '../../state';
import { controllerServicesFeatureKey } from '../../state/controller-services';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { ControllerServiceEntity } from '../../../../state/shared';
import { ComponentType } from '@nifi/shared';
import {
    clearControllerServiceBulletins,
    loadControllerServices
} from '../../state/controller-services/controller-services.actions';
import { BreadcrumbEntity } from '../../state/shared';
import { FormsModule } from '@angular/forms';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ControllerServicesState } from '../../state/controller-services';
import { Breadcrumbs } from '../common/breadcrumbs/breadcrumbs.component';
import { ControllerServiceTable } from '../../../../ui/common/controller-service/controller-service-table/controller-service-table.component';

describe('ControllerServices', () => {
    // Mock data factories
    function createMockControllerServiceEntity(): ControllerServiceEntity {
        return {
            id: 'test-controller-service-id',
            uri: 'test-uri',
            bulletins: [
                {
                    id: 1,
                    sourceId: 'test-controller-service-id',
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
                        sourceId: 'test-controller-service-id',
                        groupId: 'test-group-id',
                        sourceType: 'CONTROLLER_SERVICE',
                        sourceName: 'Test Controller Service'
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
            parentGroupId: 'test-process-group-id'
        } as ControllerServiceEntity;
    }

    function createMockBreadcrumb(
        options: {
            id?: string;
            name?: string;
            canRead?: boolean;
            canWrite?: boolean;
            hasParent?: boolean;
        } = {}
    ): BreadcrumbEntity {
        const {
            id = 'current-pg-id',
            name = 'Current Process Group',
            canRead = true,
            canWrite = true,
            hasParent = true
        } = options;

        const breadcrumb: BreadcrumbEntity = {
            id,
            permissions: {
                canRead,
                canWrite
            },
            versionedFlowState: '',
            breadcrumb: {
                id,
                name
            }
        };

        if (hasParent) {
            breadcrumb.parentBreadcrumb = {
                id: 'parent-pg-id',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                versionedFlowState: '',
                breadcrumb: {
                    id: 'parent-pg-id',
                    name: 'Parent Process Group'
                }
            };
        }

        return breadcrumb;
    }

    function createMockControllerService(
        options: {
            id?: string;
            name?: string;
            parentGroupId?: string;
            canRead?: boolean;
            canWrite?: boolean;
        } = {}
    ): ControllerServiceEntity {
        const {
            id = 'service-1',
            name = 'Test Service',
            parentGroupId = 'current-pg-id',
            canRead = true,
            canWrite = true
        } = options;

        return {
            id,
            parentGroupId,
            permissions: { canRead, canWrite },
            uri: `nifi-api/controller-services/${id}`,
            revision: {
                version: 0
            },
            component: {
                id,
                name,
                type: 'TestService',
                bundle: { group: 'test', artifact: 'test', version: '1.0' },
                state: 'DISABLED',
                persistsState: false,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                properties: {},
                descriptors: {},
                referencingComponents: [],
                validationErrors: []
            }
        } as ControllerServiceEntity;
    }

    function createMockControllerServices(): ControllerServiceEntity[] {
        return [
            createMockControllerService({ id: 'service-1', name: 'Service 1', parentGroupId: 'current-pg-id' }),
            createMockControllerService({ id: 'service-2', name: 'Service 2', parentGroupId: 'parent-pg-id' }),
            createMockControllerService({ id: 'service-3', name: 'Service 3', parentGroupId: 'current-pg-id' })
        ];
    }

    function createMockState(
        options: {
            breadcrumb?: BreadcrumbEntity;
            controllerServices?: ControllerServiceEntity[];
            processGroupId?: string;
        } = {}
    ): ControllerServicesState {
        const {
            breadcrumb = createMockBreadcrumb(),
            controllerServices = createMockControllerServices(),
            processGroupId = 'current-pg-id'
        } = options;

        return {
            processGroupId,
            breadcrumb,
            controllerServices,
            parameterContext: null,
            saving: false,
            loadedTimestamp: '2024-01-01T00:00:00Z',
            status: 'success'
        };
    }

    // Setup function for component configuration
    interface SetupOptions {
        controllerServicesState?: ControllerServicesState;
        mockStore?: any;
    }

    async function setup(options: SetupOptions = {}) {
        const { controllerServicesState = createMockState(), mockStore = {} } = options;

        const testInitialState = {
            [canvasFeatureKey]: {
                [controllerServicesFeatureKey]: controllerServicesState
            },
            flowConfiguration: {
                flowConfiguration: {
                    supportsManagedAuthorizer: false,
                    supportsConfigurableAuthorizer: false,
                    supportsConfigurableUsersAndGroups: false,
                    supportsRestrictedComponents: false,
                    supportsVersioning: false
                }
            },
            currentUser: {
                user: {
                    identity: 'test-user',
                    anonymous: false
                }
            },
            extensionTypes: {
                controllerServiceTypes: []
            },
            error: {
                bannerErrors: {}
            },
            ...mockStore
        };

        await TestBed.configureTestingModule({
            declarations: [ControllerServices],
            imports: [
                RouterTestingModule,
                HttpClientTestingModule,
                FormsModule,
                MatCheckboxModule,
                MockComponent(Navigation),
                MockComponent(NgxSkeletonLoaderComponent),
                MockComponent(Breadcrumbs),
                MockComponent(ControllerServiceTable)
            ],
            providers: [provideMockStore({ initialState: testInitialState })],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        const fixture = TestBed.createComponent(ControllerServices);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);
        fixture.detectChanges();

        return { component, fixture, store };
    }

    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
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
        it('should dispatch loadControllerServices action when refreshControllerServiceListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            // Set the current process group ID
            (component as any).currentProcessGroupId = 'test-process-group-id';

            component.refreshControllerServiceListing();

            expect(store.dispatch).toHaveBeenCalledWith(
                loadControllerServices({
                    request: {
                        processGroupId: 'test-process-group-id'
                    }
                })
            );
        });

        it('should dispatch clearControllerServiceBulletins action when clearBulletinsControllerService is called', async () => {
            const { component, store } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            jest.spyOn(store, 'dispatch');

            component.clearBulletinsControllerService(mockControllerServiceEntity);

            expect(store.dispatch).toHaveBeenCalledWith(
                clearControllerServiceBulletins({
                    request: {
                        uri: mockControllerServiceEntity.uri,
                        fromTimestamp: '2023-10-08T12:00:00.000Z',
                        componentId: mockControllerServiceEntity.id,
                        componentType: ComponentType.ControllerService
                    }
                })
            );
        });
    });

    describe('Breadcrumb and scope functions', () => {
        it('should format scope correctly for controller service entity', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: true },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const formatScopeFn = component.formatScope(mockBreadcrumb);
            const result = formatScopeFn(mockControllerServiceEntity);

            expect(result).toBe('Test Process Group');
        });

        it('should return entity ID when breadcrumb cannot be read', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: false, canWrite: false },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const formatScopeFn = component.formatScope(mockBreadcrumb);
            const result = formatScopeFn(mockControllerServiceEntity);

            expect(result).toBe('test-process-group-id');
        });

        it('should return empty string when breadcrumb is not found', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            mockControllerServiceEntity.parentGroupId = 'different-group-id';
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: true },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const formatScopeFn = component.formatScope(mockBreadcrumb);
            const result = formatScopeFn(mockControllerServiceEntity);

            expect(result).toBe('');
        });

        it('should return true when entity is defined by current group', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: true },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const definedByCurrentGroupFn = component.definedByCurrentGroup(mockBreadcrumb);
            const result = definedByCurrentGroupFn(mockControllerServiceEntity);

            expect(result).toBe(true);
        });

        it('should return false when entity is not defined by current group', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            mockControllerServiceEntity.parentGroupId = 'different-group-id';
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: true },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const definedByCurrentGroupFn = component.definedByCurrentGroup(mockBreadcrumb);
            const result = definedByCurrentGroupFn(mockControllerServiceEntity);

            expect(result).toBe(false);
        });
    });

    describe('Permission functions', () => {
        it('should return true when user can modify parent group', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: true },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const canModifyParentFn = component.canModifyParent(mockBreadcrumb);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(true);
        });

        it('should return false when user cannot modify parent group', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: false },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const canModifyParentFn = component.canModifyParent(mockBreadcrumb);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(false);
        });

        it('should return false when breadcrumb is not found for permission check', async () => {
            const { component } = await setup();
            const mockControllerServiceEntity = createMockControllerServiceEntity();
            mockControllerServiceEntity.parentGroupId = 'different-group-id';
            const mockBreadcrumb = {
                id: 'test-process-group-id',
                breadcrumb: { id: 'test-process-group-id', name: 'Test Process Group' },
                permissions: { canRead: true, canWrite: true },
                parentBreadcrumb: undefined,
                versionedFlowState: 'UNVERSIONED'
            };

            const canModifyParentFn = component.canModifyParent(mockBreadcrumb);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(false);
        });
    });

    describe('Filtering functionality', () => {
        it('should initialize with filter unchecked', async () => {
            const { component } = await setup();
            expect(component.showCurrentScopeOnly()).toBe(false);
        });

        it('should return all services when filter is disabled', async () => {
            const { component } = await setup();
            component.showCurrentScopeOnly.set(false);

            const filtered = component.filteredControllerServices();
            expect(filtered.length).toBe(3);
            expect(filtered.map((s) => s.id)).toEqual(['service-1', 'service-2', 'service-3']);
        });

        it('should return only current scope services when filter is enabled', async () => {
            const { component } = await setup();
            component.showCurrentScopeOnly.set(true);

            const filtered = component.filteredControllerServices();
            expect(filtered.length).toBe(2);
            expect(filtered.every((service) => service.parentGroupId === 'current-pg-id')).toBe(true);
            expect(filtered.map((s) => s.id)).toEqual(['service-1', 'service-3']);
        });

        it('should return empty array when no service state is available', async () => {
            const { component, store } = await setup();

            // Manually set the store state to undefined for controller services
            store.setState({
                [canvasFeatureKey]: {
                    [controllerServicesFeatureKey]: undefined
                },
                flowConfiguration: {
                    flowConfiguration: {
                        supportsManagedAuthorizer: false,
                        supportsConfigurableAuthorizer: false,
                        supportsConfigurableUsersAndGroups: false,
                        supportsRestrictedComponents: false,
                        supportsVersioning: false
                    }
                },
                currentUser: {
                    user: {
                        identity: 'test-user',
                        anonymous: false
                    }
                },
                extensionTypes: {
                    controllerServiceTypes: []
                },
                error: {
                    bannerErrors: {}
                }
            });

            const filtered = component.filteredControllerServices();
            expect(filtered).toEqual([]);
        });

        it('should toggle filter state when toggleFilter is called', async () => {
            const { component } = await setup();
            expect(component.showCurrentScopeOnly()).toBe(false);

            component.toggleFilter();
            expect(component.showCurrentScopeOnly()).toBe(true);

            component.toggleFilter();
            expect(component.showCurrentScopeOnly()).toBe(false);
        });

        it('should generate correct tooltip text with readable process group name', async () => {
            const breadcrumb = createMockBreadcrumb({ name: 'Test Process Group' });
            const { component } = await setup({
                controllerServicesState: createMockState({ breadcrumb })
            });

            const tooltip = component.getFilterTooltip(breadcrumb);
            expect(tooltip).toBe(
                "Only show the Controller Services defined in the current Process Group: 'Test Process Group'"
            );
        });

        it('should generate tooltip with ID when process group name is not readable', async () => {
            const breadcrumb = createMockBreadcrumb({
                id: 'test-pg-id',
                name: 'Test Process Group',
                canRead: false
            });
            const { component } = await setup({
                controllerServicesState: createMockState({ breadcrumb })
            });

            const tooltip = component.getFilterTooltip(breadcrumb);
            expect(tooltip).toBe(
                "Only show the Controller Services defined in the current Process Group: 'test-pg-id'"
            );
        });
    });

    describe('shouldShowFilter computed property', () => {
        it('should return true when breadcrumb has parent (not root)', async () => {
            const { component } = await setup();
            expect(component.shouldShowFilter()).toBe(true);
        });

        it('should return false when breadcrumb has no parent (root process group)', async () => {
            const rootBreadcrumb = createMockBreadcrumb({
                id: 'root-pg-id',
                name: 'Root Process Group',
                hasParent: false
            });
            const { component } = await setup({
                controllerServicesState: createMockState({ breadcrumb: rootBreadcrumb })
            });

            expect(component.shouldShowFilter()).toBe(false);
        });

        it('should return false when no service state is available', async () => {
            const { component, store } = await setup();

            // Manually set the store state to undefined for controller services
            store.setState({
                [canvasFeatureKey]: {
                    [controllerServicesFeatureKey]: undefined
                },
                flowConfiguration: {
                    flowConfiguration: {
                        supportsManagedAuthorizer: false,
                        supportsConfigurableAuthorizer: false,
                        supportsConfigurableUsersAndGroups: false,
                        supportsRestrictedComponents: false,
                        supportsVersioning: false
                    }
                },
                currentUser: {
                    user: {
                        identity: 'test-user',
                        anonymous: false
                    }
                },
                extensionTypes: {
                    controllerServiceTypes: []
                },
                error: {
                    bannerErrors: {}
                }
            });

            expect(component.shouldShowFilter()).toBe(false);
        });
    });

    describe('Template integration', () => {
        it('should render checkbox with correct initial state', async () => {
            const { fixture } = await setup();

            const checkbox = fixture.nativeElement.querySelector('[data-qa="current-scope-filter"]');
            expect(checkbox).toBeTruthy();
        });

        it('should render label with correct text', async () => {
            const { fixture } = await setup();

            const label = fixture.nativeElement.querySelector('[data-qa="current-scope-label"]');
            expect(label).toBeTruthy();
            expect(label.textContent.trim()).toBe('Show current scope only');
        });

        it('should render help icon with tooltip', async () => {
            const { fixture } = await setup();

            const helpIcon = fixture.nativeElement.querySelector('[data-qa="current-scope-help"]');
            expect(helpIcon).toBeTruthy();
        });

        it('should toggle filter when label is clicked', async () => {
            const { component, fixture } = await setup();

            const label = fixture.nativeElement.querySelector('[data-qa="current-scope-label"]');
            expect(component.showCurrentScopeOnly()).toBe(false);

            label.click();
            expect(component.showCurrentScopeOnly()).toBe(true);
        });

        it('should not render filter elements when at root process group', async () => {
            const rootBreadcrumb = createMockBreadcrumb({
                id: 'root-pg-id',
                name: 'Root Process Group',
                hasParent: false
            });
            const { fixture } = await setup({
                controllerServicesState: createMockState({ breadcrumb: rootBreadcrumb })
            });

            const checkbox = fixture.nativeElement.querySelector('[data-qa="current-scope-filter"]');
            const label = fixture.nativeElement.querySelector('[data-qa="current-scope-label"]');
            const helpIcon = fixture.nativeElement.querySelector('[data-qa="current-scope-help"]');

            expect(checkbox).toBeFalsy();
            expect(label).toBeFalsy();
            expect(helpIcon).toBeFalsy();
        });
    });
});
