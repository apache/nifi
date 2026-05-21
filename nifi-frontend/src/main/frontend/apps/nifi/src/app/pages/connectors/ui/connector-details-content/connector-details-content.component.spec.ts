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
import { ConnectorDetailsContent } from './connector-details-content.component';
import { ConnectorEntity, ConnectorState } from '@nifi/shared';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { By } from '@angular/platform-browser';

describe('ConnectorDetailsContent', () => {
    let component: ConnectorDetailsContent;
    let fixture: ComponentFixture<ConnectorDetailsContent>;

    const mockConnector: ConnectorEntity = {
        id: 'test-connector-id',
        uri: 'http://localhost:4200/nifi-api/connectors/test-connector-id',
        permissions: {
            canRead: true,
            canWrite: true
        },
        status: {
            runStatus: 'RUNNING',
            validationStatus: 'VALID'
        },
        component: {
            id: 'test-connector-id',
            name: 'Test Connector',
            type: 'org.apache.nifi.connector.TestConnector',
            state: ConnectorState.RUNNING,
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-test-connector',
                version: '1.0.0'
            },
            activeConfiguration: {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Basic Configuration',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Connection Settings',
                                propertyDescriptors: {
                                    hostname: {
                                        name: 'hostname',
                                        description: 'The hostname to connect to',
                                        type: 'STRING',
                                        required: true
                                    }
                                },
                                propertyValues: {
                                    hostname: {
                                        value: 'localhost',
                                        valueType: 'STRING_LITERAL'
                                    }
                                }
                            }
                        ]
                    }
                ]
            },
            managedProcessGroupId: 'test-pg-id',
            availableActions: [
                { name: 'START', description: 'Start action', allowed: true },
                { name: 'STOP', description: 'Stop action', allowed: true },
                { name: 'CONFIGURE', description: 'Configure action', allowed: true },
                { name: 'DELETE', description: 'Delete action', allowed: true }
            ]
        },
        bulletins: [],
        revision: {
            version: 1
        }
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ConnectorDetailsContent, MatIconTestingModule],
            providers: [provideMockStore()]
        }).compileComponents();

        fixture = TestBed.createComponent(ConnectorDetailsContent);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        fixture.componentRef.setInput('connector', mockConnector);
        fixture.detectChanges();
        expect(component).toBeTruthy();
    });

    describe('configuration sections', () => {
        it('should display configuration step sections', () => {
            fixture.componentRef.setInput('connector', mockConnector);
            fixture.detectChanges();

            const stepSection = fixture.nativeElement.querySelector('[data-qa="step-section"]');
            expect(stepSection).toBeTruthy();
        });

        it('should display step name', () => {
            fixture.componentRef.setInput('connector', mockConnector);
            fixture.detectChanges();

            const stepName = fixture.nativeElement.querySelector('[data-qa="step-name"]');
            expect(stepName?.textContent?.trim()).toBe('Basic Configuration');
        });

        it('should show no-config-state when configuration steps are empty', () => {
            const connectorWithEmptyConfig = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: {
                        configurationStepConfigurations: []
                    }
                }
            };
            fixture.componentRef.setInput('connector', connectorWithEmptyConfig);
            fixture.detectChanges();

            const noConfigState = fixture.nativeElement.querySelector('[data-qa="no-config-state"]');
            expect(noConfigState).toBeTruthy();
        });

        it('should show empty-state when no activeConfiguration is present', () => {
            const connectorWithoutConfig = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: undefined
                }
            };
            fixture.componentRef.setInput('connector', connectorWithoutConfig);
            fixture.detectChanges();

            const emptyState = fixture.nativeElement.querySelector('[data-qa="empty-state"]');
            expect(emptyState).toBeTruthy();
        });
    });

    describe('hideGroupName binding', () => {
        it('should set hideGroupName to true when step has a single property group', () => {
            fixture.componentRef.setInput('connector', mockConnector);
            fixture.detectChanges();

            const card = fixture.nativeElement.querySelector('property-group-card');
            expect(card).toBeTruthy();
            const cardDebug = fixture.debugElement.query(By.css('property-group-card'));
            expect(cardDebug.componentInstance.hideGroupName()).toBe(true);
        });

        it('should set hideGroupName to false when step has multiple property groups', () => {
            const connectorWithMultipleGroups: any = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: {
                        configurationStepConfigurations: [
                            {
                                configurationStepName: 'Config Step',
                                dependencies: [],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Group 1',
                                        propertyDescriptors: {
                                            field1: { name: 'field1', type: 'STRING', required: true }
                                        },
                                        propertyValues: {
                                            field1: { value: 'val1', valueType: 'STRING_LITERAL' }
                                        }
                                    },
                                    {
                                        propertyGroupName: 'Group 2',
                                        propertyDescriptors: {
                                            field2: { name: 'field2', type: 'STRING', required: false }
                                        },
                                        propertyValues: {
                                            field2: { value: 'val2', valueType: 'STRING_LITERAL' }
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            };
            fixture.componentRef.setInput('connector', connectorWithMultipleGroups);
            fixture.detectChanges();

            const cards = fixture.debugElement.queryAll(By.css('property-group-card'));
            expect(cards.length).toBe(2);
            expect(cards[0].componentInstance.hideGroupName()).toBe(false);
            expect(cards[1].componentInstance.hideGroupName()).toBe(false);
        });
    });

    describe('filteredStepConfigurations', () => {
        it('should return empty array when connector has no activeConfiguration', () => {
            const connectorWithoutConfig: ConnectorEntity = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: undefined
                }
            };
            fixture.componentRef.setInput('connector', connectorWithoutConfig);

            expect(component.filteredStepConfigurations()).toEqual([]);
        });

        it('should return all properties when none have dependencies', () => {
            fixture.componentRef.setInput('connector', mockConnector);

            const filtered = component.filteredStepConfigurations();
            expect(filtered.length).toBe(1);
            expect(Object.keys(filtered[0].propertyGroupConfigurations[0].propertyDescriptors)).toContain('hostname');
        });

        it('should filter out hidden properties based on dependencies', () => {
            const connectorWithDependencies: any = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: {
                        configurationStepConfigurations: [
                            {
                                configurationStepName: 'Config Step',
                                dependencies: [],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Settings',
                                        propertyDescriptors: {
                                            enableFeature: {
                                                name: 'enableFeature',
                                                description: 'Enable feature',
                                                type: 'STRING',
                                                required: true
                                            },
                                            featureOption: {
                                                name: 'featureOption',
                                                description: 'Feature option',
                                                type: 'STRING',
                                                required: false,
                                                dependencies: [
                                                    { propertyName: 'enableFeature', dependentValues: ['yes'] }
                                                ]
                                            }
                                        },
                                        propertyValues: {
                                            enableFeature: { value: 'no', valueType: 'STRING_LITERAL' }
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            };
            fixture.componentRef.setInput('connector', connectorWithDependencies);

            const filtered = component.filteredStepConfigurations();
            const descriptors = filtered[0].propertyGroupConfigurations[0].propertyDescriptors;

            expect(Object.keys(descriptors)).toContain('enableFeature');
            expect(Object.keys(descriptors)).not.toContain('featureOption');
        });

        it('should include visible dependent properties when condition is met', () => {
            const connectorWithDependencies: any = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: {
                        configurationStepConfigurations: [
                            {
                                configurationStepName: 'Config Step',
                                dependencies: [],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Settings',
                                        propertyDescriptors: {
                                            enableFeature: {
                                                name: 'enableFeature',
                                                description: 'Enable feature',
                                                type: 'STRING',
                                                required: true
                                            },
                                            featureOption: {
                                                name: 'featureOption',
                                                description: 'Feature option',
                                                type: 'STRING',
                                                required: false,
                                                dependencies: [
                                                    { propertyName: 'enableFeature', dependentValues: ['yes'] }
                                                ]
                                            }
                                        },
                                        propertyValues: {
                                            enableFeature: { value: 'yes', valueType: 'STRING_LITERAL' }
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            };
            fixture.componentRef.setInput('connector', connectorWithDependencies);

            const filtered = component.filteredStepConfigurations();
            const descriptors = filtered[0].propertyGroupConfigurations[0].propertyDescriptors;

            expect(Object.keys(descriptors)).toContain('enableFeature');
            expect(Object.keys(descriptors)).toContain('featureOption');
        });

        it('should filter out hidden steps based on step dependencies', () => {
            const connectorWithStepDependencies: any = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: {
                        configurationStepConfigurations: [
                            {
                                configurationStepName: 'Step 1',
                                dependencies: [],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Settings',
                                        propertyDescriptors: {
                                            enableAdvanced: {
                                                name: 'enableAdvanced',
                                                type: 'STRING',
                                                required: true
                                            }
                                        },
                                        propertyValues: {
                                            enableAdvanced: { value: 'no', valueType: 'STRING_LITERAL' }
                                        }
                                    }
                                ]
                            },
                            {
                                configurationStepName: 'Step 2 - Advanced',
                                dependencies: [
                                    { stepName: 'Step 1', propertyName: 'enableAdvanced', dependentValues: ['yes'] }
                                ],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Advanced Settings',
                                        propertyDescriptors: {
                                            advancedOption: {
                                                name: 'advancedOption',
                                                type: 'STRING',
                                                required: false
                                            }
                                        },
                                        propertyValues: {}
                                    }
                                ]
                            }
                        ]
                    }
                }
            };
            fixture.componentRef.setInput('connector', connectorWithStepDependencies);

            const filtered = component.filteredStepConfigurations();

            expect(filtered.length).toBe(1);
            expect(filtered[0].configurationStepName).toBe('Step 1');
        });

        it('should include visible steps when step dependency is met', () => {
            const connectorWithStepDependencies: any = {
                ...mockConnector,
                component: {
                    ...mockConnector.component,
                    activeConfiguration: {
                        configurationStepConfigurations: [
                            {
                                configurationStepName: 'Step 1',
                                dependencies: [],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Settings',
                                        propertyDescriptors: {
                                            enableAdvanced: {
                                                name: 'enableAdvanced',
                                                type: 'STRING',
                                                required: true
                                            }
                                        },
                                        propertyValues: {
                                            enableAdvanced: { value: 'yes', valueType: 'STRING_LITERAL' }
                                        }
                                    }
                                ]
                            },
                            {
                                configurationStepName: 'Step 2 - Advanced',
                                dependencies: [
                                    { stepName: 'Step 1', propertyName: 'enableAdvanced', dependentValues: ['yes'] }
                                ],
                                propertyGroupConfigurations: [
                                    {
                                        propertyGroupName: 'Advanced Settings',
                                        propertyDescriptors: {
                                            advancedOption: {
                                                name: 'advancedOption',
                                                type: 'STRING',
                                                required: false
                                            }
                                        },
                                        propertyValues: {}
                                    }
                                ]
                            }
                        ]
                    }
                }
            };
            fixture.componentRef.setInput('connector', connectorWithStepDependencies);

            const filtered = component.filteredStepConfigurations();

            expect(filtered.length).toBe(2);
            expect(filtered[0].configurationStepName).toBe('Step 1');
            expect(filtered[1].configurationStepName).toBe('Step 2 - Advanced');
        });
    });
});
