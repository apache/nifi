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
import { provideMockStore } from '@ngrx/store/testing';
import { of } from 'rxjs';
import {
    ConnectionDialogEndpoint,
    CreateConnection as CreateConnectionPayload,
    CreateConnectionDialogRequest
} from '../../../../state/flow-shared';
import { ComponentType, ProcessorConfigDTO, RemoteProcessGroupPortDTO } from '@nifi/shared';
import { DocumentedType } from '../../../../state/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { Client } from '../../../../service/client.service';

describe('CreateConnection', () => {
    const permissions = { canRead: true, canWrite: true };
    const position = { x: 0, y: 0 };

    function entityFields(id: string) {
        return {
            id,
            uri: id,
            revision: { version: 0 },
            permissions,
            position,
            operatePermissions: permissions
        };
    }

    function groupCounts() {
        return {
            runningCount: 0,
            stoppedCount: 0,
            invalidCount: 0,
            disabledCount: 0,
            activeRemotePortCount: 0,
            inactiveRemotePortCount: 0,
            upToDateCount: 0,
            locallyModifiedCount: 0,
            staleCount: 0,
            locallyModifiedAndStaleCount: 0,
            syncFailureCount: 0,
            localInputPortCount: 0,
            localOutputPortCount: 0,
            publicInputPortCount: 0,
            publicOutputPortCount: 0,
            inputPortCount: 0,
            outputPortCount: 0
        };
    }

    function processorConfig(): ProcessorConfigDTO {
        return {
            schedulingPeriod: '0 sec',
            schedulingStrategy: 'TIMER_DRIVEN',
            executionNode: 'ALL',
            penaltyDuration: '30 sec',
            yieldDuration: '1 sec',
            bulletinLevel: 'WARN',
            runDurationMillis: 0,
            concurrentlySchedulableTaskCount: 1,
            autoTerminatedRelationships: [],
            lossTolerant: false,
            retryCount: 0,
            retriedRelationships: [],
            backoffMechanism: 'PENALIZE_FLOWFILE',
            maxBackoffPeriod: '10 mins'
        };
    }

    function remotePort(id: string, name: string, groupId: string): RemoteProcessGroupPortDTO {
        return {
            id,
            name,
            groupId,
            concurrentlySchedulableTaskCount: 1,
            transmitting: false,
            useCompression: false,
            exists: true,
            targetRunning: false,
            connected: false,
            batchSettings: { count: 0 }
        };
    }

    function createMockInputPort(id = 'input-port-id'): ConnectionDialogEndpoint {
        return {
            id,
            componentType: ComponentType.InputPort,
            entity: {
                ...entityFields(id),
                portType: 'INPUT_PORT',
                component: {
                    id,
                    position,
                    parentGroupId: 'group-id',
                    name: 'Input Port',
                    state: 'STOPPED',
                    type: 'INPUT_PORT',
                    portFunction: 'STANDARD',
                    concurrentlySchedulableTaskCount: 1
                }
            }
        };
    }

    function createMockProcessor(id = 'processor-id'): ConnectionDialogEndpoint {
        return {
            id,
            componentType: ComponentType.Processor,
            entity: {
                ...entityFields(id),
                inputRequirement: 'INPUT_REQUIRED',
                physicalState: 'STOPPED',
                component: {
                    id,
                    position,
                    parentGroupId: 'group-id',
                    name: 'Test Processor',
                    type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
                    bundle: { group: 'org.apache.nifi', artifact: 'nifi-standard-nar', version: '2.0.0' },
                    state: 'STOPPED',
                    relationships: [
                        {
                            name: 'success',
                            description: 'Success relationship',
                            autoTerminate: false,
                            retry: false
                        }
                    ],
                    supportsParallelProcessing: false,
                    supportsBatching: false,
                    supportsSensitiveDynamicProperties: false,
                    supportsBacklogReporting: false,
                    persistsState: false,
                    restricted: false,
                    deprecated: false,
                    extensionMissing: false,
                    executionNodeRestricted: false,
                    multipleVersionsAvailable: false,
                    inputRequirement: 'INPUT_REQUIRED',
                    physicalState: 'STOPPED',
                    config: processorConfig(),
                    validationStatus: 'VALID'
                }
            }
        };
    }

    function createMockProcessGroup(id = 'process-group-id'): ConnectionDialogEndpoint {
        const counts = groupCounts();
        return {
            id,
            componentType: ComponentType.ProcessGroup,
            entity: {
                ...entityFields(id),
                ...counts,
                resolvedExecutionEngine: 'STANDARD',
                component: {
                    id,
                    position,
                    parentGroupId: 'group-id',
                    name: 'Test Process Group',
                    flowfileConcurrency: 'UNBOUNDED',
                    flowfileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
                    defaultFlowFileExpiration: '0 sec',
                    defaultBackPressureObjectThreshold: 10000,
                    defaultBackPressureDataSizeThreshold: '1 GB',
                    executionEngine: 'INHERITED',
                    resolvedExecutionEngine: 'STANDARD',
                    maxConcurrentTasks: 1,
                    statelessFlowTimeout: '1 min',
                    statelessGroupScheduledState: 'STOPPED',
                    ...counts
                }
            }
        };
    }

    function createMockRemoteProcessGroup(id = 'remote-group-id'): ConnectionDialogEndpoint {
        return {
            id,
            componentType: ComponentType.RemoteProcessGroup,
            entity: {
                ...entityFields(id),
                inputPortCount: 1,
                outputPortCount: 1,
                component: {
                    id,
                    position,
                    parentGroupId: 'group-id',
                    name: 'Remote Process Group',
                    targetUris: 'https://localhost:8443/nifi',
                    communicationsTimeout: '30 sec',
                    yieldDuration: '10 sec',
                    transportProtocol: 'RAW',
                    transmitting: false,
                    activeRemoteInputPortCount: 0,
                    inactiveRemoteInputPortCount: 1,
                    activeRemoteOutputPortCount: 0,
                    inactiveRemoteOutputPortCount: 1,
                    contents: {
                        outputPorts: [remotePort('output-1', 'Output 1', id)],
                        inputPorts: [remotePort('input-1', 'Input 1', id)]
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
            source?: ConnectionDialogEndpoint;
            destination?: ConnectionDialogEndpoint;
            bends?: Array<{ x: number; y: number }>;
        } = {}
    ) {
        const testSource = options.source || createMockInputPort();
        const testDestination = options.destination || createMockProcessor();

        const testDialogData: CreateConnectionDialogRequest = {
            request: {
                source: testSource,
                destination: testDestination,
                bends: options.bends
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
                        [errorFeatureKey]: initialErrorState
                    }
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: vi.fn().mockReturnValue(false)
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        const fixture = TestBed.createComponent(CreateConnection);
        const component = fixture.componentInstance;
        component.breadcrumbs$ = of(createMockBreadcrumb());
        component.saving$ = of(false);
        component.availablePrioritizers$ = of(createMockPrioritizers());

        fixture.detectChanges();

        return { component, fixture };
    }

    beforeEach(() => {
        vi.clearAllMocks();
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
        it('emits a create connection payload whose revision uses the local client id', async () => {
            const { component } = await setup();
            const client = TestBed.inject(Client);
            const emitted: CreateConnectionPayload[] = [];
            component.createConnection.subscribe((request) => emitted.push(request));

            component.submitCreateConnection('root');

            expect(emitted).toHaveLength(1);
            expect(emitted[0].payload.revision).toEqual({
                version: 0,
                clientId: client.getClientId()
            });
        });

        it('should include relationships for Processor source type', async () => {
            const source = createMockProcessor('source-processor');
            const { component, fixture } = await setup({ source });

            component.createConnectionForm.patchValue({ relationships: ['success'] });
            fixture.detectChanges();

            const emitted: CreateConnectionPayload[] = [];
            component.createConnection.subscribe((request) => emitted.push(request));

            component.submitCreateConnection('root');

            expect(emitted[0].payload.component.selectedRelationships).toEqual(['success']);
        });

        it('preserves endpoint, bend, acknowledgment, and load-balance payload fields', async () => {
            const bends = [
                { x: 10, y: 20 },
                { x: 30, y: 40 }
            ];
            const source = createMockRemoteProcessGroup('source-group');
            const destination = createMockRemoteProcessGroup('destination-group');
            const { component } = await setup({ source, destination, bends });
            component.loadBalanceChanged('PARTITION_BY_ATTRIBUTE');
            component.createConnectionForm.patchValue({
                name: 'Prioritized',
                loadBalanceStrategy: 'PARTITION_BY_ATTRIBUTE',
                partitionAttribute: 'tenant',
                compression: 'COMPRESS_ATTRIBUTES_ONLY',
                prioritizers: ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer']
            });
            const emitted: CreateConnectionPayload[] = [];
            component.createConnection.subscribe((request) => emitted.push(request));

            component.submitCreateConnection('root');

            expect(emitted[0].payload).toEqual(
                expect.objectContaining({
                    disconnectedNodeAcknowledged: false,
                    component: expect.objectContaining({
                        name: 'Prioritized',
                        source: {
                            groupId: 'source-group',
                            id: 'output-1',
                            type: 'REMOTE_OUTPUT_PORT'
                        },
                        destination: {
                            groupId: 'destination-group',
                            id: 'input-1',
                            type: 'REMOTE_INPUT_PORT'
                        },
                        bends,
                        loadBalanceStrategy: 'PARTITION_BY_ATTRIBUTE',
                        loadBalancePartitionAttribute: 'tenant',
                        loadBalanceCompression: 'COMPRESS_ATTRIBUTES_ONLY',
                        prioritizers: ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'],
                        flowFileExpiration: '0 sec',
                        backPressureObjectThreshold: 10000,
                        backPressureDataSizeThreshold: '1 GB'
                    })
                })
            );
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
