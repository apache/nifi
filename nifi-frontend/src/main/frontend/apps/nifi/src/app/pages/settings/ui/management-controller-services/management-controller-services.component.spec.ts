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
import { ManagementControllerServices } from './management-controller-services.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/management-controller-services/management-controller-services.reducer';
import { managementControllerServicesFeatureKey } from '../../state/management-controller-services';
import { settingsFeatureKey } from '../../state';
import { ControllerServiceEntity } from '../../../../state/shared';
import { ComponentType } from '@nifi/shared';
import { CurrentUser, currentUserFeatureKey } from '../../../../state/current-user';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';
import {
    clearControllerServiceBulletins,
    loadManagementControllerServices
} from '../../state/management-controller-services/management-controller-services.actions';

describe('ManagementControllerServices', () => {
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
            }
        } as ControllerServiceEntity;
    }

    function createMockCurrentUser(canRead: boolean = true, canWrite: boolean = true): CurrentUser {
        return {
            identity: 'test-user',
            anonymous: false,
            logoutSupported: true,
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
                canRead,
                canWrite
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
            canVersionFlows: true
        } as CurrentUser;
    }

    // Setup function for component configuration
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [ManagementControllerServices],
            providers: [
                provideMockStore({
                    initialState: {
                        [settingsFeatureKey]: {
                            [managementControllerServicesFeatureKey]: {
                                ...initialState
                            }
                        },
                        [currentUserFeatureKey]: fromCurrentUser.initialState
                    }
                })
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(ManagementControllerServices);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);
        fixture.detectChanges();

        return { component, fixture, store };
    }

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should dispatch loadManagementControllerServices action on ngOnInit', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnInit();

            expect(store.dispatch).toHaveBeenCalledWith(loadManagementControllerServices());
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
        it('should dispatch loadManagementControllerServices action when refreshControllerServiceListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.refreshControllerServiceListing();

            expect(store.dispatch).toHaveBeenCalledWith(loadManagementControllerServices());
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

    describe('Scope and group functions', () => {
        it('should return "Controller" for formatScope', async () => {
            const { component } = await setup();

            const result = component.formatScope();

            expect(result).toBe('Controller');
        });

        it('should return true for definedByCurrentGroup', async () => {
            const { component } = await setup();

            const result = component.definedByCurrentGroup();

            expect(result).toBe(true);
        });
    });

    describe('Permission functions', () => {
        it('should return true when user can modify parent (has controller read and write permissions)', async () => {
            const { component } = await setup();
            const mockCurrentUser = createMockCurrentUser(true, true);
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            const canModifyParentFn = component.canModifyParent(mockCurrentUser);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(true);
        });

        it('should return false when user cannot read controller', async () => {
            const { component } = await setup();
            const mockCurrentUser = createMockCurrentUser(false, true);
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            const canModifyParentFn = component.canModifyParent(mockCurrentUser);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(false);
        });

        it('should return false when user cannot write to controller', async () => {
            const { component } = await setup();
            const mockCurrentUser = createMockCurrentUser(true, false);
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            const canModifyParentFn = component.canModifyParent(mockCurrentUser);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(false);
        });

        it('should return false when user has no controller permissions', async () => {
            const { component } = await setup();
            const mockCurrentUser = createMockCurrentUser(false, false);
            const mockControllerServiceEntity = createMockControllerServiceEntity();

            const canModifyParentFn = component.canModifyParent(mockCurrentUser);
            const result = canModifyParentFn(mockControllerServiceEntity);

            expect(result).toBe(false);
        });
    });
});
