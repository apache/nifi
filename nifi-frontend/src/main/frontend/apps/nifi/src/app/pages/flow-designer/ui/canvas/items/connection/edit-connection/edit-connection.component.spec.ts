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
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../../state/current-user';
import { canvasFeatureKey } from '../../../../../state';
import { flowFeatureKey } from '../../../../../state/flow';

import { EditConnectionComponent } from './edit-connection.component';
import { EditConnectionDialogRequest } from '../../../../../state/flow';
import { ComponentType } from '@nifi/shared';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { selectPrioritizerTypes } from '../../../../../../../state/extension-types/extension-types.selectors';
import { selectBreadcrumbs, selectSaving } from '../../../../../state/flow/flow.selectors';
import { updateConnection } from '../../../../../state/flow/flow.actions';

describe('EditConnectionComponent', () => {
    // Mock data factories
    function createMockConnection(
        sourceType: string = 'INPUT_PORT',
        destinationType: string = 'OUTPUT_PORT',
        options: any = {}
    ) {
        return {
            id: options.id || 'connection-id',
            source: {
                id: options.sourceId || 'source-id',
                type: sourceType,
                groupId: options.sourceGroupId || 'source-group-id',
                name: options.sourceName || 'Source Component'
            },
            destination: {
                id: options.destinationId || 'destination-id',
                type: destinationType,
                groupId: options.destinationGroupId || 'destination-group-id',
                name: options.destinationName || 'Destination Component'
            },
            name: options.name !== undefined ? options.name : 'Test Connection',
            backPressureObjectThreshold: options.backPressureObjectThreshold || 10000,
            backPressureDataSizeThreshold: options.backPressureDataSizeThreshold || '1 GB',
            flowFileExpiration: options.flowFileExpiration || '0 sec',
            prioritizers: options.prioritizers || [],
            loadBalanceStrategy: options.loadBalanceStrategy || 'DO_NOT_LOAD_BALANCE',
            loadBalancePartitionAttribute: options.loadBalancePartitionAttribute || '',
            loadBalanceCompression: options.loadBalanceCompression || 'DO_NOT_COMPRESS',
            selectedRelationships: options.selectedRelationships || []
        };
    }

    function createMockDialogRequest(
        connection: any,
        permissions: any = { canRead: true, canWrite: true },
        newDestination?: any
    ): EditConnectionDialogRequest {
        return {
            type: ComponentType.Connection,
            uri: `https://localhost:4200/nifi-api/connections/${connection.id}`,
            entity: {
                revision: { version: 0 },
                id: connection.id,
                uri: `https://localhost:4200/nifi-api/connections/${connection.id}`,
                permissions,
                component: connection
            },
            newDestination
        };
    }

    function createMockBreadcrumb(canRead: boolean = true) {
        return {
            id: 'breadcrumb-id',
            permissions: { canRead, canWrite: true },
            breadcrumb: { id: 'breadcrumb-id', name: 'Test Breadcrumb' },
            versionedFlowState: 'UP_TO_DATE'
        };
    }

    function createMockPrioritizerTypes() {
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
            dialogRequest?: EditConnectionDialogRequest;
            mockStore?: any;
        } = {}
    ) {
        const defaultConnection = createMockConnection();
        const defaultDialogRequest = options.dialogRequest || createMockDialogRequest(defaultConnection);

        const storeState = {
            ...initialState,
            ...options.mockStore
        };

        await TestBed.configureTestingModule({
            imports: [EditConnectionComponent, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: defaultDialogRequest },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: storeState
                        }
                    }
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);

        // Setup mock selectors
        store.overrideSelector(selectPrioritizerTypes, createMockPrioritizerTypes());
        store.overrideSelector(selectSaving, false);
        store.overrideSelector(selectBreadcrumbs, createMockBreadcrumb());

        const fixture = TestBed.createComponent(EditConnectionComponent);
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

        it('should initialize with basic connection data', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.source).toEqual(connection.source);
            expect(component.sourceType).toBe(ComponentType.InputPort);
            expect(component.destinationType).toBe(ComponentType.OutputPort);
            expect(component.destinationId).toBe(connection.destination.id);
        });

        it('should initialize form with connection values', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                name: 'My Connection',
                flowFileExpiration: '30 sec',
                backPressureObjectThreshold: 5000,
                loadBalanceStrategy: 'ROUND_ROBIN'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.editConnectionForm.get('name')?.value).toBe('My Connection');
            expect(component.editConnectionForm.get('flowFileExpiration')?.value).toBe('30 sec');
            expect(component.editConnectionForm.get('backPressureObjectThreshold')?.value).toBe(5000);
            expect(component.editConnectionForm.get('loadBalanceStrategy')?.value).toBe('ROUND_ROBIN');
        });

        it('should set readonly state based on permissions', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection, { canRead: true, canWrite: false });
            const { component } = await setup({ dialogRequest });

            expect(component.connectionReadonly).toBe(true);
        });
    });

    describe('Source component type logic', () => {
        it('should handle Processor source type', async () => {
            const connection = createMockConnection('PROCESSOR', 'OUTPUT_PORT', {
                selectedRelationships: ['success', 'failure']
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.sourceType).toBe(ComponentType.Processor);
            expect(component.editConnectionForm.get('relationships')).toBeTruthy();
            expect(component.editConnectionForm.get('relationships')?.value).toEqual(['success', 'failure']);
        });

        it('should handle ProcessGroup source type', async () => {
            const connection = createMockConnection('OUTPUT_PORT', 'OUTPUT_PORT');
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.sourceType).toBe(ComponentType.ProcessGroup);
            expect(component.editConnectionForm.get('source')).toBeTruthy();
            expect(component.editConnectionForm.get('source')?.disabled).toBe(true);
        });

        it('should handle RemoteProcessGroup source type', async () => {
            const connection = createMockConnection('REMOTE_OUTPUT_PORT', 'OUTPUT_PORT');
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.sourceType).toBe(ComponentType.RemoteProcessGroup);
            expect(component.editConnectionForm.get('source')).toBeTruthy();
        });

        it('should handle InputPort source type', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT');
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.sourceType).toBe(ComponentType.InputPort);
            expect(component.editConnectionForm.get('relationships')).toBeFalsy();
        });
    });

    describe('Destination component type logic', () => {
        it('should handle ProcessGroup destination type', async () => {
            const connection = createMockConnection('INPUT_PORT', 'INPUT_PORT');
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.destinationType).toBe(ComponentType.ProcessGroup);
            expect(component.editConnectionForm.get('destination')).toBeTruthy();
            expect(component.editConnectionForm.get('destination')?.value).toBe(connection.destination.id);
        });

        it('should handle RemoteProcessGroup destination type', async () => {
            const connection = createMockConnection('INPUT_PORT', 'REMOTE_INPUT_PORT');
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.destinationType).toBe(ComponentType.RemoteProcessGroup);
            expect(component.editConnectionForm.get('destination')).toBeTruthy();
        });

        it('should handle Processor destination type', async () => {
            const connection = createMockConnection('INPUT_PORT', 'PROCESSOR');
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.destinationType).toBe(ComponentType.Processor);
            expect(component.editConnectionForm.get('destination')).toBeFalsy();
        });
    });

    describe('Load balance strategy logic', () => {
        it('should handle DO_NOT_LOAD_BALANCE strategy', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.loadBalancePartitionAttributeRequired).toBe(false);
            expect(component.loadBalanceCompressionRequired).toBe(false);
            expect(component.editConnectionForm.get('partitionAttribute')).toBeFalsy();
            expect(component.editConnectionForm.get('compression')).toBeFalsy();
        });

        it('should handle PARTITION_BY_ATTRIBUTE strategy', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'PARTITION_BY_ATTRIBUTE',
                loadBalancePartitionAttribute: 'filename',
                loadBalanceCompression: 'GZIP'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.loadBalancePartitionAttributeRequired).toBe(true);
            expect(component.loadBalanceCompressionRequired).toBe(true);
            expect(component.editConnectionForm.get('partitionAttribute')).toBeTruthy();
            expect(component.editConnectionForm.get('partitionAttribute')?.value).toBe('filename');
            expect(component.editConnectionForm.get('compression')).toBeTruthy();
            expect(component.editConnectionForm.get('compression')?.value).toBe('GZIP');
        });

        it('should handle ROUND_ROBIN strategy', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'ROUND_ROBIN',
                loadBalanceCompression: 'SNAPPY'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            expect(component.loadBalancePartitionAttributeRequired).toBe(false);
            expect(component.loadBalanceCompressionRequired).toBe(true);
            expect(component.editConnectionForm.get('partitionAttribute')).toBeFalsy();
            expect(component.editConnectionForm.get('compression')).toBeTruthy();
        });

        it('should add partition attribute control when strategy changes to PARTITION_BY_ATTRIBUTE', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            component.loadBalanceChanged('PARTITION_BY_ATTRIBUTE');

            expect(component.loadBalancePartitionAttributeRequired).toBe(true);
            expect(component.loadBalanceCompressionRequired).toBe(true);
            expect(component.editConnectionForm.get('partitionAttribute')).toBeTruthy();
            expect(component.editConnectionForm.get('compression')).toBeTruthy();
        });

        it('should remove controls when strategy changes to DO_NOT_LOAD_BALANCE', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'PARTITION_BY_ATTRIBUTE',
                loadBalancePartitionAttribute: 'filename'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component } = await setup({ dialogRequest });

            component.loadBalanceChanged('DO_NOT_LOAD_BALANCE');

            expect(component.loadBalancePartitionAttributeRequired).toBe(false);
            expect(component.loadBalanceCompressionRequired).toBe(false);
            expect(component.editConnectionForm.get('partitionAttribute')).toBeFalsy();
            expect(component.editConnectionForm.get('compression')).toBeFalsy();
        });
    });

    describe('New destination logic', () => {
        it('should initialize with new destination when provided', async () => {
            const connection = createMockConnection();
            const newDestination = {
                type: ComponentType.Processor,
                id: 'new-processor-id',
                groupId: 'new-group-id',
                name: 'New Processor'
            };
            const dialogRequest = createMockDialogRequest(connection, undefined, newDestination);
            const { component } = await setup({ dialogRequest });

            expect(component.destinationType).toBe(ComponentType.Processor);
            expect(component.destinationId).toBe('new-processor-id');
            expect(component.destinationGroupId).toBe('new-group-id');
            expect(component.destinationName).toBe('New Processor');
            expect(component.previousDestination).toEqual(connection.destination);
        });
    });

    describe('Edit connection method', () => {
        it('should dispatch updateConnection action when editConnection is called', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection);
            const { component, store } = await setup({ dialogRequest });

            const dispatchSpy = jest.spyOn(store, 'dispatch');

            component.editConnection();

            expect(dispatchSpy).toHaveBeenCalledWith(
                updateConnection({
                    request: expect.objectContaining({
                        id: connection.id,
                        type: ComponentType.Connection,
                        payload: expect.objectContaining({
                            component: expect.objectContaining({
                                id: connection.id
                            })
                        })
                    })
                })
            );
        });

        it('should include relationships for Processor source type', async () => {
            const connection = createMockConnection('PROCESSOR', 'OUTPUT_PORT', {
                selectedRelationships: ['success']
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component, store } = await setup({ dialogRequest });

            const dispatchSpy = jest.spyOn(store, 'dispatch');

            component.editConnection();

            expect(dispatchSpy).toHaveBeenCalledWith(
                updateConnection({
                    request: expect.objectContaining({
                        payload: expect.objectContaining({
                            component: expect.objectContaining({
                                selectedRelationships: ['success']
                            })
                        })
                    })
                })
            );
        });
    });

    describe('Template logic', () => {
        it('should display "Edit Connection" title when not readonly', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection, { canRead: true, canWrite: true });
            const { fixture } = await setup({ dialogRequest });

            const dialogTitle = fixture.nativeElement.querySelector('[data-qa="dialog-title"]');
            expect(dialogTitle).toBeTruthy();
            expect(dialogTitle.textContent.trim()).toBe('Edit Connection');
        });

        it('should display "Connection Details" title when readonly', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection, { canRead: true, canWrite: false });
            const { fixture } = await setup({ dialogRequest });

            const dialogTitle = fixture.nativeElement.querySelector('[data-qa="dialog-title"]');
            expect(dialogTitle).toBeTruthy();
            expect(dialogTitle.textContent.trim()).toBe('Connection Details');
        });

        it('should display edit connection form', async () => {
            const { fixture } = await setup();

            const editForm = fixture.nativeElement.querySelector('[data-qa="edit-connection-form"]');
            expect(editForm).toBeTruthy();

            const connectionTabs = fixture.nativeElement.querySelector('[data-qa="connection-tabs"]');
            expect(connectionTabs).toBeTruthy();
        });

        it('should display partition attribute section when PARTITION_BY_ATTRIBUTE is selected', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'PARTITION_BY_ATTRIBUTE',
                loadBalancePartitionAttribute: 'filename'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component, fixture } = await setup({ dialogRequest });

            // Switch to Settings tab (index 1) where the partition attribute section is displayed
            component.selectedIndex = 1;
            fixture.detectChanges();

            const partitionSection = fixture.nativeElement.querySelector('[data-qa="partition-attribute-section"]');
            expect(partitionSection).toBeTruthy();

            const partitionInput = fixture.nativeElement.querySelector('[data-qa="partition-attribute-input"]');
            expect(partitionInput).toBeTruthy();
        });

        it('should not display partition attribute section when DO_NOT_LOAD_BALANCE is selected', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component, fixture } = await setup({ dialogRequest });

            // Switch to Settings tab (index 1) where the partition attribute section would be displayed
            component.selectedIndex = 1;
            fixture.detectChanges();

            const partitionSection = fixture.nativeElement.querySelector('[data-qa="partition-attribute-section"]');
            expect(partitionSection).toBeFalsy();
        });

        it('should display compression section when load balance strategy requires compression', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'ROUND_ROBIN',
                loadBalanceCompression: 'GZIP'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component, fixture } = await setup({ dialogRequest });

            // Switch to Settings tab (index 1) where the compression section is displayed
            component.selectedIndex = 1;
            fixture.detectChanges();

            const compressionSection = fixture.nativeElement.querySelector('[data-qa="compression-section"]');
            expect(compressionSection).toBeTruthy();

            const compressionSelect = fixture.nativeElement.querySelector('[data-qa="compression-select"]');
            expect(compressionSelect).toBeTruthy();
        });

        it('should not display compression section when DO_NOT_LOAD_BALANCE is selected', async () => {
            const connection = createMockConnection('INPUT_PORT', 'OUTPUT_PORT', {
                loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE'
            });
            const dialogRequest = createMockDialogRequest(connection);
            const { component, fixture } = await setup({ dialogRequest });

            // Switch to Settings tab (index 1) where the compression section would be displayed
            component.selectedIndex = 1;
            fixture.detectChanges();

            const compressionSection = fixture.nativeElement.querySelector('[data-qa="compression-section"]');
            expect(compressionSection).toBeFalsy();
        });

        it('should display Close button when readonly', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection, { canRead: true, canWrite: false });
            const { fixture } = await setup({ dialogRequest });

            const closeButton = fixture.nativeElement.querySelector('[data-qa="close-button"]');
            expect(closeButton).toBeTruthy();
            expect(closeButton.textContent.trim()).toBe('Close');

            // Should not show Cancel/Apply buttons
            const cancelButton = fixture.nativeElement.querySelector('[data-qa="cancel-button"]');
            const applyButton = fixture.nativeElement.querySelector('[data-qa="apply-button"]');
            expect(cancelButton).toBeFalsy();
            expect(applyButton).toBeFalsy();
        });

        it('should display Cancel and Apply buttons when not readonly', async () => {
            const connection = createMockConnection();
            const dialogRequest = createMockDialogRequest(connection, { canRead: true, canWrite: true });
            const { fixture } = await setup({ dialogRequest });

            const cancelButton = fixture.nativeElement.querySelector('[data-qa="cancel-button"]');
            const applyButton = fixture.nativeElement.querySelector('[data-qa="apply-button"]');
            expect(cancelButton).toBeTruthy();
            expect(applyButton).toBeTruthy();

            // Should not show Close button
            const closeButton = fixture.nativeElement.querySelector('[data-qa="close-button"]');
            expect(closeButton).toBeFalsy();
        });
    });
});
