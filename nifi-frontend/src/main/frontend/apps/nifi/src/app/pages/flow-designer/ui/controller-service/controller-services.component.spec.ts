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
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { canvasFeatureKey } from '../../state';
import { controllerServicesFeatureKey } from '../../state/controller-services';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { ControllerServiceEntity } from '../../../../state/shared';
import { BreadcrumbEntity } from '../../state/shared';
import { FormsModule } from '@angular/forms';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ControllerServicesState } from '../../state/controller-services';
import { Breadcrumbs } from '../common/breadcrumbs/breadcrumbs.component';
import { ControllerServiceTable } from '../../../../ui/common/controller-service/controller-service-table/controller-service-table.component';

describe('ControllerServices', () => {
    // Mock data factories
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

        const initialState = {
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
            providers: [provideMockStore({ initialState })],
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
