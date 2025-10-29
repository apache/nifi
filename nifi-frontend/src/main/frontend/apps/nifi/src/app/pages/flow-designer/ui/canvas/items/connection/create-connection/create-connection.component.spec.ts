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

import { CreateConnection } from './create-connection.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { CreateConnectionDialogRequest } from '../../../../../state/flow';
import { ComponentType } from '@nifi/shared';
import { DocumentedType } from '../../../../../../../state/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../../state/current-user';
import { canvasFeatureKey } from '../../../../../state';
import { flowFeatureKey } from '../../../../../state/flow';
import { selectBreadcrumbs, selectSaving } from '../../../../../state/flow/flow.selectors';
import { selectPrioritizerTypes } from '../../../../../../../state/extension-types/extension-types.selectors';
import { createConnection } from '../../../../../state/flow/flow.actions';

describe('CreateConnection', () => {
    // Mock data factories
    function createMockInputPort(id: string = 'input-port-id'): any {
        return {
            id,
            componentType: ComponentType.InputPort,
            entity: {
                id,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id,
                    name: 'Input Port',
                    parentGroupId: 'group-id'
                }
            }
        };
    }

    function createMockProcessor(id: string = 'processor-id'): any {
        return {
            id,
            componentType: ComponentType.Processor,
            entity: {
                id,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id,
                    name: 'Test Processor',
                    parentGroupId: 'group-id',
                    relationships: [{ name: 'success', description: 'Success relationship', autoTerminate: false }]
                }
            }
        };
    }

    function createMockProcessGroup(id: string = 'process-group-id'): any {
        return {
            id,
            componentType: ComponentType.ProcessGroup,
            entity: {
                id,
                permissions: { canRead: true, canWrite: true },
                component: {
                    id,
                    name: 'Test Process Group',
                    parentGroupId: 'group-id'
                }
            }
        };
    }

    function createMockRemoteProcessGroup(id: string = 'remote-group-id'): any {
        return {
            id,
            componentType: ComponentType.RemoteProcessGroup,
            entity: {
                id,
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id,
                    name: 'Remote Process Group',
                    parentGroupId: 'group-id',
                    contents: {
                        outputPorts: [{ id: 'output-1', name: 'Output 1' }],
                        inputPorts: [{ id: 'input-1', name: 'Input 1' }]
                    }
                }
            }
        };
    }

    function createMockBreadcrumb() {
        return {
            id: 'root',
            permissions: { canRead: true, canWrite: true },
            versionedFlowState: 'UP_TO_DATE',
            breadcrumb: { id: 'root', name: 'Root Group' }
        };
    }

    function createMockPrioritizers(): DocumentedType[] {
        return [
            {
                type: 'org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer',
                bundle: { group: 'org.apache.nifi', artifact: 'nifi-framework-nar', version: '2.0.0-SNAPSHOT' },
                restricted: false,
                tags: []
            }
        ];
    }

    // Setup function for component configuration
    async function setup(
        options: {
            source?: any;
            destination?: any;
        } = {}
    ) {
        const testSource = options.source || createMockInputPort();
        const testDestination = options.destination || createMockProcessor();

        const testDialogData: CreateConnectionDialogRequest = {
            request: {
                source: testSource,
                destination: testDestination
            },
            defaults: {
                flowfileExpiration: '0 sec',
                objectThreshold: 10000,
                dataSizeThreshold: '1 GB'
            }
        };

        await TestBed.configureTestingModule({
            imports: [CreateConnection, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: testDialogData },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState
                        }
                    }
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn().mockReturnValue(false)
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);

        // Setup required selectors
        store.overrideSelector(selectBreadcrumbs, createMockBreadcrumb());
        store.overrideSelector(selectSaving, false);
        store.overrideSelector(selectPrioritizerTypes, createMockPrioritizers());

        const fixture = TestBed.createComponent(CreateConnection);
        const component = fixture.componentInstance;

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

        it('should initialize with InputPort source and Processor destination', async () => {
            const { component } = await setup();
            expect(component.source).toBeDefined();
            expect(component.destination).toBeDefined();
            expect(component.source.componentType).toBe(ComponentType.InputPort);
            expect(component.destination.componentType).toBe(ComponentType.Processor);
        });

        it('should initialize form with default values', async () => {
            const { component } = await setup();
            expect(component.createConnectionForm.get('flowFileExpiration')?.value).toBe('0 sec');
            expect(component.createConnectionForm.get('backPressureObjectThreshold')?.value).toBe(10000);
            expect(component.createConnectionForm.get('backPressureDataSizeThreshold')?.value).toBe('1 GB');
        });
    });

    describe('Source component types', () => {
        it('should handle Processor source type', async () => {
            const source = createMockProcessor('source-processor');
            const { component } = await setup({ source });

            expect(component.source.componentType).toBe(ComponentType.Processor);
            expect(component.createConnectionForm.get('relationships')).toBeTruthy();
        });

        it('should handle ProcessGroup source type', async () => {
            const source = createMockProcessGroup('source-group');
            const { component } = await setup({ source });

            expect(component.source.componentType).toBe(ComponentType.ProcessGroup);
        });

        it('should handle RemoteProcessGroup source type', async () => {
            const source = createMockRemoteProcessGroup('source-remote');
            const { component } = await setup({ source });

            expect(component.source.componentType).toBe(ComponentType.RemoteProcessGroup);
        });
    });

    describe('Destination component types', () => {
        it('should handle Processor destination type', async () => {
            const destination = createMockProcessor('dest-processor');
            const { component } = await setup({ destination });

            expect(component.destination.componentType).toBe(ComponentType.Processor);
        });

        it('should handle ProcessGroup destination type', async () => {
            const destination = createMockProcessGroup('dest-group');
            const { component } = await setup({ destination });

            expect(component.destination.componentType).toBe(ComponentType.ProcessGroup);
        });

        it('should handle RemoteProcessGroup destination type', async () => {
            const destination = createMockRemoteProcessGroup('dest-remote');
            const { component } = await setup({ destination });

            expect(component.destination.componentType).toBe(ComponentType.RemoteProcessGroup);
        });
    });

    describe('Load balance strategy logic', () => {
        it('should handle DO_NOT_LOAD_BALANCE by default', async () => {
            const { component } = await setup();

            expect(component.createConnectionForm.get('loadBalanceStrategy')?.value).toBe('DO_NOT_LOAD_BALANCE');
            expect(component.loadBalancePartitionAttributeRequired).toBe(false);
            expect(component.loadBalanceCompressionRequired).toBe(false);
        });

        it('should handle PARTITION_BY_ATTRIBUTE strategy change', async () => {
            const { component } = await setup();

            component.loadBalanceChanged('PARTITION_BY_ATTRIBUTE');

            expect(component.loadBalancePartitionAttributeRequired).toBe(true);
            expect(component.loadBalanceCompressionRequired).toBe(true);
            expect(component.createConnectionForm.get('partitionAttribute')).toBeTruthy();
            expect(component.createConnectionForm.get('compression')).toBeTruthy();
        });

        it('should handle ROUND_ROBIN strategy change', async () => {
            const { component } = await setup();

            component.loadBalanceChanged('ROUND_ROBIN');

            expect(component.loadBalancePartitionAttributeRequired).toBe(false);
            expect(component.loadBalanceCompressionRequired).toBe(true);
            expect(component.createConnectionForm.get('compression')).toBeTruthy();
        });
    });

    describe('Create connection method', () => {
        it('should dispatch createConnection action when createConnection is called', async () => {
            const { component, store } = await setup();

            const dispatchSpy = jest.spyOn(store, 'dispatch');

            component.createConnection('root');

            expect(dispatchSpy).toHaveBeenCalledWith(
                createConnection({
                    request: expect.objectContaining({
                        payload: expect.any(Object)
                    })
                })
            );
        });

        it('should include relationships for Processor source type', async () => {
            const source = createMockProcessor('source-processor');
            const { component, store, fixture } = await setup({ source });

            // Set relationships
            component.createConnectionForm.patchValue({ relationships: ['success'] });
            fixture.detectChanges();

            const dispatchSpy = jest.spyOn(store, 'dispatch');

            component.createConnection('root');

            expect(dispatchSpy).toHaveBeenCalled();
            const dispatchCall = dispatchSpy.mock.calls[0][0] as any;
            expect(dispatchCall.type).toBe('[Canvas] Create Connection');
            expect(dispatchCall.request.payload.component.selectedRelationships).toEqual(['success']);
        });
    });

    describe('Template logic', () => {
        it('should display "Create Connection" title', async () => {
            const { fixture } = await setup();

            const dialogTitle = fixture.nativeElement.querySelector('[data-qa="dialog-title"]');
            expect(dialogTitle).toBeTruthy();
            expect(dialogTitle.textContent.trim()).toBe('Create Connection');
        });

        it('should display create connection form', async () => {
            const { fixture } = await setup();

            const form = fixture.nativeElement.querySelector('[data-qa="create-connection-form"]');
            expect(form).toBeTruthy();

            const tabs = fixture.nativeElement.querySelector('[data-qa="connection-tabs"]');
            expect(tabs).toBeTruthy();
        });

        it('should display dialog actions with Cancel and Add buttons', async () => {
            const { fixture } = await setup();

            const dialogActions = fixture.nativeElement.querySelector('[data-qa="dialog-actions"]');
            expect(dialogActions).toBeTruthy();

            const cancelButton = fixture.nativeElement.querySelector('[data-qa="cancel-button"]');
            expect(cancelButton).toBeTruthy();
            expect(cancelButton.textContent.trim()).toBe('Cancel');

            const addButton = fixture.nativeElement.querySelector('[data-qa="add-button"]');
            expect(addButton).toBeTruthy();
        });

        it('should disable Add button when form is invalid', async () => {
            const { component, fixture } = await setup();

            // Make form invalid
            component.createConnectionForm.setErrors({ invalid: true });
            fixture.detectChanges();

            const addButton = fixture.nativeElement.querySelector('[data-qa="add-button"]');
            expect(addButton.disabled).toBe(true);
        });
    });
});
