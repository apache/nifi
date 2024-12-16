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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateConnection } from './create-connection.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { CreateConnectionDialogRequest } from '../../../../../state/flow';
import { ComponentType } from '@nifi/shared';
import { DocumentedType } from '../../../../../../../state/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { of } from 'rxjs';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';

describe('CreateConnection', () => {
    let component: CreateConnection;
    let fixture: ComponentFixture<CreateConnection>;

    const data: CreateConnectionDialogRequest = {
        request: {
            source: {
                id: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                componentType: ComponentType.InputPort,
                entity: {
                    revision: {
                        version: 0
                    },
                    id: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                    uri: 'https://localhost:4200/nifi-api/input-ports/a67bf99d-018b-1000-611d-2993eb2f64b8',
                    position: {
                        x: 1160,
                        y: -320
                    },
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    bulletins: [],
                    component: {
                        id: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                        versionedComponentId: '77458ab4-8e53-3855-a682-c787a2705b9d',
                        parentGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                        position: {
                            x: 1160,
                            y: -320
                        },
                        name: 'in',
                        state: 'STOPPED',
                        type: 'INPUT_PORT',
                        transmitting: false,
                        concurrentlySchedulableTaskCount: 1,
                        allowRemoteAccess: true,
                        portFunction: 'STANDARD'
                    },
                    status: {
                        id: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                        groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                        name: 'in',
                        transmitting: false,
                        runStatus: 'Stopped',
                        statsLastRefreshed: '14:03:53 EST',
                        aggregateSnapshot: {
                            id: 'a67bf99d-018b-1000-611d-2993eb2f64b8',
                            groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                            name: 'in',
                            activeThreadCount: 0,
                            flowFilesIn: 0,
                            bytesIn: 0,
                            input: '0 (0 bytes)',
                            flowFilesOut: 0,
                            bytesOut: 0,
                            output: '0 (0 bytes)',
                            runStatus: 'Stopped'
                        }
                    },
                    portType: 'INPUT_PORT',
                    operatePermissions: {
                        canRead: false,
                        canWrite: false
                    },
                    allowRemoteAccess: true,
                    type: 'InputPort',
                    dimensions: {
                        width: 240,
                        height: 80
                    }
                }
            },
            destination: {
                id: 'ca0a0504-018b-1000-5917-2b063e8946b9',
                componentType: ComponentType.Processor,
                entity: {
                    revision: {
                        clientId: 'd8e8a955-018b-1000-915e-a59d0e7933ef',
                        version: 6
                    },
                    id: 'ca0a0504-018b-1000-5917-2b063e8946b9',
                    uri: 'https://localhost:4200/nifi-api/processors/ca0a0504-018b-1000-5917-2b063e8946b9',
                    position: {
                        x: 144,
                        y: -280
                    },
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    bulletins: [],
                    component: {
                        id: 'ca0a0504-018b-1000-5917-2b063e8946b9',
                        versionedComponentId: 'e499c564-caa6-3c53-ad8b-371745868b27',
                        parentGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                        position: {
                            x: 144,
                            y: -280
                        },
                        name: 'UpdateAttribute',
                        type: 'org.apache.nifi.processors.attributes.UpdateAttribute',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-update-attribute-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'STOPPED',
                        style: {
                            'background-color': '#966969'
                        },
                        relationships: [
                            {
                                name: 'success',
                                description: 'All successful FlowFiles are routed to this relationship',
                                autoTerminate: false,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: true,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: true,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_REQUIRED',
                        config: {
                            properties: {
                                'Delete Attributes Expression': null,
                                'Store State': 'Do not store state',
                                'Stateful Variables Initial Value': null,
                                'canonical-value-lookup-cache-size': '100',
                                asdf: 'qwer'
                            },
                            descriptors: {
                                'Delete Attributes Expression': {
                                    name: 'Delete Attributes Expression',
                                    displayName: 'Delete Attributes Expression',
                                    description:
                                        'Regular expression for attributes to be deleted from FlowFiles.  Existing attributes that match will be deleted regardless of whether they are updated by this processor.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                                    dependencies: []
                                },
                                'Store State': {
                                    name: 'Store State',
                                    displayName: 'Store State',
                                    description:
                                        "Select whether or not state will be stored. Selecting 'Stateless' will offer the default functionality of purely updating the attributes on a FlowFile in a stateless manner. Selecting a stateful option will not only store the attributes on the FlowFile but also in the Processors state. See the 'Stateful Usage' topic of the 'Additional Details' section of this processor's documentation for more information",
                                    defaultValue: 'Do not store state',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'Do not store state',
                                                value: 'Do not store state'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Store state locally',
                                                value: 'Store state locally'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'Stateful Variables Initial Value': {
                                    name: 'Stateful Variables Initial Value',
                                    displayName: 'Stateful Variables Initial Value',
                                    description:
                                        'If using state to set/reference variables then this value is used to set the initial value of the stateful variable. This will only be used in the @OnScheduled method when state does not contain a value for the variable. This is required if running statefully but can be empty if needed.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'canonical-value-lookup-cache-size': {
                                    name: 'canonical-value-lookup-cache-size',
                                    displayName: 'Cache Value Lookup Cache Size',
                                    description:
                                        'Specifies how many canonical lookup values should be stored in the cache',
                                    defaultValue: '100',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                asdf: {
                                    name: 'asdf',
                                    displayName: 'asdf',
                                    description: '',
                                    required: false,
                                    sensitive: false,
                                    dynamic: true,
                                    supportsEl: true,
                                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                                    dependencies: []
                                }
                            },
                            schedulingPeriod: '10 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 25,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: [],
                            comments: '',
                            customUiUrl: '/nifi-update-attribute-ui-2.0.0-SNAPSHOT',
                            lossTolerant: false,
                            defaultConcurrentTasks: {
                                TIMER_DRIVEN: '1',
                                CRON_DRIVEN: '1'
                            },
                            defaultSchedulingPeriod: {
                                TIMER_DRIVEN: '0 sec',
                                CRON_DRIVEN: '* * * * * ?'
                            },
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationStatus: 'VALID',
                        extensionMissing: false
                    },
                    inputRequirement: 'INPUT_REQUIRED',
                    status: {
                        groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                        id: 'ca0a0504-018b-1000-5917-2b063e8946b9',
                        name: 'UpdateAttribute',
                        runStatus: 'Stopped',
                        statsLastRefreshed: '14:03:53 EST',
                        aggregateSnapshot: {
                            id: 'ca0a0504-018b-1000-5917-2b063e8946b9',
                            groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                            name: 'UpdateAttribute',
                            type: 'UpdateAttribute',
                            runStatus: 'Stopped',
                            executionNode: 'ALL',
                            bytesRead: 0,
                            bytesWritten: 0,
                            read: '0 bytes',
                            written: '0 bytes',
                            flowFilesIn: 0,
                            bytesIn: 0,
                            input: '0 (0 bytes)',
                            flowFilesOut: 0,
                            bytesOut: 0,
                            output: '0 (0 bytes)',
                            taskCount: 0,
                            tasksDurationNanos: 0,
                            tasks: '0',
                            tasksDuration: '00:00:00.000',
                            activeThreadCount: 0,
                            terminatedThreadCount: 0
                        }
                    },
                    operatePermissions: {
                        canRead: false,
                        canWrite: false
                    },
                    type: 'Processor',
                    dimensions: {
                        width: 352,
                        height: 128
                    }
                }
            }
        },
        defaults: {
            flowfileExpiration: '0 sec',
            objectThreshold: 10000,
            dataSizeThreshold: '1 GB'
        }
    };

    const parameterContexts: DocumentedType[] = [
        {
            type: 'org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-framework-nar',
                version: '2.0.0-SNAPSHOT'
            },
            restricted: false,
            tags: []
        },
        {
            type: 'org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-framework-nar',
                version: '2.0.0-SNAPSHOT'
            },
            restricted: false,
            tags: []
        },
        {
            type: 'org.apache.nifi.prioritizer.OldestFlowFileFirstPrioritizer',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-framework-nar',
                version: '2.0.0-SNAPSHOT'
            },
            restricted: false,
            tags: []
        },
        {
            type: 'org.apache.nifi.prioritizer.PriorityAttributePrioritizer',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-framework-nar',
                version: '2.0.0-SNAPSHOT'
            },
            restricted: false,
            tags: []
        }
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateConnection, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({ initialState }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateConnection);
        component = fixture.componentInstance;
        component.availablePrioritizers$ = of(parameterContexts);
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
