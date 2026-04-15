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

import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ConnectorConfigurationSummaryStep } from './connector-configuration-summary-step.component';
import { ConfigVerificationResult, ConnectorConfiguration } from '../../types';
import { By } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('ConnectorConfigurationSummaryStep', () => {
    let component: ConnectorConfigurationSummaryStep;
    let fixture: ComponentFixture<ConnectorConfigurationSummaryStep>;

    const mockConfiguration: ConnectorConfiguration = {
        configurationStepConfigurations: [
            {
                configurationStepName: 'Source Database Configuration',
                dependencies: [],
                propertyGroupConfigurations: [
                    {
                        propertyGroupName: 'Connection Settings',
                        propertyDescriptors: {
                            hostname: {
                                name: 'hostname',
                                type: 'STRING',
                                required: true
                            },
                            port: {
                                name: 'port',
                                type: 'INTEGER',
                                required: true
                            }
                        },
                        propertyValues: {
                            hostname: {
                                valueType: 'STRING_LITERAL',
                                value: 'localhost'
                            },
                            port: {
                                valueType: 'STRING_LITERAL',
                                value: '5432'
                            }
                        }
                    },
                    {
                        propertyGroupName: 'Authentication',
                        propertyDescriptors: {
                            username: {
                                name: 'username',
                                type: 'STRING',
                                required: true
                            }
                        },
                        propertyValues: {
                            username: {
                                valueType: 'STRING_LITERAL',
                                value: 'admin'
                            }
                        }
                    }
                ]
            },
            {
                configurationStepName: 'Destination Database Configuration',
                dependencies: [],
                propertyGroupConfigurations: [
                    {
                        propertyGroupName: 'Database Settings',
                        propertyDescriptors: {
                            account: {
                                name: 'account',
                                type: 'STRING',
                                required: true
                            }
                        },
                        propertyValues: {
                            account: {
                                valueType: 'STRING_LITERAL',
                                value: 'my-account'
                            }
                        }
                    }
                ]
            }
        ]
    };

    const mockConfigurationWithMissingRequiredFields: ConnectorConfiguration = {
        configurationStepConfigurations: [
            {
                configurationStepName: 'Source Configuration',
                dependencies: [],
                propertyGroupConfigurations: [
                    {
                        propertyGroupName: 'Connection',
                        propertyDescriptors: {
                            hostname: {
                                name: 'hostname',
                                type: 'STRING',
                                required: true
                            },
                            port: {
                                name: 'port',
                                type: 'INTEGER',
                                required: false
                            }
                        },
                        propertyValues: {
                            hostname: {
                                valueType: 'STRING_LITERAL',
                                value: '' // Empty required field
                            },
                            port: {
                                valueType: 'STRING_LITERAL',
                                value: '5432'
                            }
                        }
                    }
                ]
            }
        ]
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ConnectorConfigurationSummaryStep, NoopAnimationsModule],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        fixture = TestBed.createComponent(ConnectorConfigurationSummaryStep);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('Loading State', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', true);
            fixture.componentRef.setInput('workingConfiguration', null);
            fixture.detectChanges();
        });

        it('should display skeleton loaders when loading', () => {
            const skeletons = fixture.debugElement.queryAll(By.css('ngx-skeleton-loader'));
            expect(skeletons.length).toBeGreaterThan(0);
        });

        it('should display correct number of skeleton step sections', () => {
            const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-name-skeleton"]'));
            expect(stepSections.length).toBe(component.skeletonSteps.length);
        });

        it('should not display actual configuration content when loading', () => {
            const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
            expect(stepSections.length).toBe(0);
        });

        it('should disable back button when loading', () => {
            const backButton = fixture.debugElement.query(By.css('[data-qa="back-button"]'));
            expect(backButton.nativeElement.disabled).toBe(true);
        });

        it('should disable apply button when loading', () => {
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            expect(applyButton.nativeElement.disabled).toBe(true);
        });
    });

    describe('Configuration Display', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();
        });

        it('should display summary title', () => {
            const title = fixture.debugElement.query(By.css('[data-qa="summary-title"]'));
            expect(title).toBeTruthy();
        });

        it('should display summary description', () => {
            const description = fixture.debugElement.query(By.css('[data-qa="summary-description"]'));
            expect(description).toBeTruthy();
        });

        it('should display all configuration steps', () => {
            const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
            expect(stepSections.length).toBe(mockConfiguration.configurationStepConfigurations.length);
        });

        it('should display step names correctly', () => {
            const stepNames = fixture.debugElement.queryAll(By.css('[data-qa="step-name"]'));
            expect(stepNames.length).toBe(2);
            expect(stepNames[0].nativeElement.textContent.trim()).toBe('Source Database Configuration');
            expect(stepNames[1].nativeElement.textContent.trim()).toBe('Destination Database Configuration');
        });

        it('should render property-group-card for each property group', () => {
            const propertyGroupCards = fixture.debugElement.queryAll(By.css('property-group-card'));
            const totalGroups = mockConfiguration.configurationStepConfigurations.reduce(
                (sum, step) => sum + step.propertyGroupConfigurations.length,
                0
            );
            expect(propertyGroupCards.length).toBe(totalGroups);
        });

        it('should pass correct property group data to property-group-card', () => {
            const firstCard = fixture.debugElement.query(By.css('property-group-card'));
            const firstGroup = mockConfiguration.configurationStepConfigurations[0].propertyGroupConfigurations[0];
            expect(firstCard.componentInstance.propertyGroup()).toEqual(firstGroup);
        });

        it('should set hideGroupName to false for steps with multiple groups', () => {
            const cards = fixture.debugElement.queryAll(By.css('property-group-card'));
            // First step has 2 groups - both should show group name
            expect(cards[0].componentInstance.hideGroupName()).toBe(false);
            expect(cards[1].componentInstance.hideGroupName()).toBe(false);
        });

        it('should set hideGroupName to true for steps with a single group', () => {
            const cards = fixture.debugElement.queryAll(By.css('property-group-card'));
            // Second step has 1 group - should hide group name
            expect(cards[2].componentInstance.hideGroupName()).toBe(true);
        });

        it('should not display skeleton loaders when configuration is loaded', () => {
            const skeletons = fixture.debugElement.queryAll(By.css('ngx-skeleton-loader'));
            expect(skeletons.length).toBe(0);
        });

        it('should not display empty state when configuration is loaded', () => {
            const emptyState = fixture.debugElement.query(By.css('[data-qa="empty-state"]'));
            expect(emptyState).toBeFalsy();
        });
    });

    describe('Empty State', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('workingConfiguration', null);
            fixture.detectChanges();
        });

        it('should display empty state when no configuration is available', () => {
            const emptyState = fixture.debugElement.query(By.css('[data-qa="empty-state"]'));
            expect(emptyState).toBeTruthy();
        });

        it('should display info icon in empty state', () => {
            const icon = fixture.debugElement.query(By.css('[data-qa="empty-state"] .fa-info-circle'));
            expect(icon).toBeTruthy();
        });

        it('should not display configuration sections in empty state', () => {
            const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
            expect(stepSections.length).toBe(0);
        });
    });

    describe('Button Interactions', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('applying', false);
            fixture.componentRef.setInput('verificationPassed', true);
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();
        });

        it('should emit confirm event when Apply button is clicked', () => {
            const confirmSpy = vi.spyOn(component.confirm, 'emit');
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));

            applyButton.nativeElement.click();

            expect(confirmSpy).toHaveBeenCalledTimes(1);
        });

        it('should emit dismiss event when Close button is clicked', () => {
            const dismissSpy = vi.spyOn(component.dismiss, 'emit');
            const closeButton = fixture.debugElement.query(By.css('[data-qa="close-button"]'));

            closeButton.nativeElement.click();

            expect(dismissSpy).toHaveBeenCalledTimes(1);
        });

        it('should emit previous event when Back button is clicked', () => {
            const previousSpy = vi.spyOn(component.previous, 'emit');
            const backButton = fixture.debugElement.query(By.css('[data-qa="back-button"]'));

            backButton.nativeElement.click();

            expect(previousSpy).toHaveBeenCalledTimes(1);
        });

        it('should enable all buttons when not loading or applying', () => {
            const closeButton = fixture.debugElement.query(By.css('[data-qa="close-button"]'));
            const backButton = fixture.debugElement.query(By.css('[data-qa="back-button"]'));
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));

            expect(closeButton.nativeElement.disabled).toBe(false);
            expect(backButton.nativeElement.disabled).toBe(false);
            expect(applyButton.nativeElement.disabled).toBe(false);
        });
    });

    describe('Applying State', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('applying', true);
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();
        });

        it('should disable Close button when applying', () => {
            const closeButton = fixture.debugElement.query(By.css('[data-qa="close-button"]'));
            expect(closeButton.nativeElement.disabled).toBe(true);
        });

        it('should disable Back button when applying', () => {
            const backButton = fixture.debugElement.query(By.css('[data-qa="back-button"]'));
            expect(backButton.nativeElement.disabled).toBe(true);
        });

        it('should disable Apply button when applying', () => {
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            expect(applyButton.nativeElement.disabled).toBe(true);
        });

        it('should have spinner-enabled span in Apply button when applying', () => {
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            // Verify the button exists and spinner directive is applied
            expect(applyButton).toBeTruthy();
            // The spinner directive creates a span element when applied
            const spans = applyButton.queryAll(By.css('span'));
            expect(spans.length).toBeGreaterThan(0);
        });
    });

    describe('Component Methods', () => {
        it('should call confirm.emit() when onConfirm is called', () => {
            const confirmSpy = vi.spyOn(component.confirm, 'emit');
            component.onConfirm();
            expect(confirmSpy).toHaveBeenCalledTimes(1);
        });

        it('should call dismiss.emit() when onDismiss is called', () => {
            const dismissSpy = vi.spyOn(component.dismiss, 'emit');
            component.onDismiss();
            expect(dismissSpy).toHaveBeenCalledTimes(1);
        });

        it('should call previous.emit() when onPrevious is called', () => {
            const previousSpy = vi.spyOn(component.previous, 'emit');
            component.onPrevious();
            expect(previousSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('Signal Inputs', () => {
        it('should accept workingConfiguration input', () => {
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            expect(component.workingConfiguration()).toEqual(mockConfiguration);
        });

        it('should accept loading input', () => {
            fixture.componentRef.setInput('loading', true);
            expect(component.loading()).toBe(true);
        });

        it('should accept applying input', () => {
            fixture.componentRef.setInput('applying', true);
            expect(component.applying()).toBe(true);
        });

        it('should default loading to false', () => {
            expect(component.loading()).toBe(false);
        });

        it('should default applying to false', () => {
            expect(component.applying()).toBe(false);
        });
    });

    describe('Skeleton Configuration', () => {
        it('should have correct skeleton steps array', () => {
            expect(component.skeletonSteps).toEqual([1, 2, 3]);
        });

        it('should have correct skeleton properties array', () => {
            expect(component.skeletonProperties).toEqual([1, 2, 3, 4, 5, 6]);
        });
    });

    describe('Accessibility', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();
        });

        it('should have aria-label on Close button', () => {
            const closeButton = fixture.debugElement.query(By.css('[data-qa="close-button"]'));
            expect(closeButton.nativeElement.hasAttribute('aria-label')).toBe(true);
        });

        it('should have aria-label on Back button', () => {
            const backButton = fixture.debugElement.query(By.css('[data-qa="back-button"]'));
            expect(backButton.nativeElement.hasAttribute('aria-label')).toBe(true);
        });

        it('should have aria-label on Apply button', () => {
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            expect(applyButton.nativeElement.hasAttribute('aria-label')).toBe(true);
        });
    });

    describe('Data QA Attributes', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();
        });

        it('should have data-qa attribute on summary title', () => {
            const title = fixture.debugElement.query(By.css('[data-qa="summary-title"]'));
            expect(title).toBeTruthy();
        });

        it('should have data-qa attribute on summary description', () => {
            const description = fixture.debugElement.query(By.css('[data-qa="summary-description"]'));
            expect(description).toBeTruthy();
        });

        it('should have data-qa attributes on all buttons', () => {
            const closeButton = fixture.debugElement.query(By.css('[data-qa="close-button"]'));
            const backButton = fixture.debugElement.query(By.css('[data-qa="back-button"]'));
            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            const verifyButton = fixture.debugElement.query(By.css('[data-qa="verify-button"]'));

            expect(closeButton).toBeTruthy();
            expect(backButton).toBeTruthy();
            expect(applyButton).toBeTruthy();
            expect(verifyButton).toBeTruthy();
        });

        it('should have data-qa attribute on step actions container', () => {
            const stepActions = fixture.debugElement.query(By.css('[data-qa="step-actions"]'));
            expect(stepActions).toBeTruthy();
        });
    });

    describe('hasMissingRequiredFields', () => {
        it('should return false when all required fields have values', () => {
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should return true when a required field is empty', () => {
            fixture.componentRef.setInput('workingConfiguration', mockConfigurationWithMissingRequiredFields);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(true);
        });

        it('should return false when configuration is null', () => {
            fixture.componentRef.setInput('workingConfiguration', null);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should return false when configuration has no steps', () => {
            const emptyConfig: ConnectorConfiguration = {
                configurationStepConfigurations: []
            };
            fixture.componentRef.setInput('workingConfiguration', emptyConfig);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should ignore non-required empty fields', () => {
            const configWithOptionalEmpty: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyDescriptors: {
                                    required_field: {
                                        name: 'required_field',
                                        type: 'STRING',
                                        required: true
                                    },
                                    optional_field: {
                                        name: 'optional_field',
                                        type: 'STRING',
                                        required: false
                                    }
                                },
                                propertyValues: {
                                    required_field: {
                                        valueType: 'STRING_LITERAL',
                                        value: 'has value'
                                    },
                                    optional_field: {
                                        valueType: 'STRING_LITERAL',
                                        value: '' // Empty but not required
                                    }
                                }
                            }
                        ]
                    }
                ]
            };
            fixture.componentRef.setInput('workingConfiguration', configWithOptionalEmpty);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should disable apply button when verification has not been run', () => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('workingConfiguration', mockConfigurationWithMissingRequiredFields);
            fixture.detectChanges();

            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            expect(applyButton.nativeElement.disabled).toBe(true);
        });

        it('should enable apply button when all required fields are populated', () => {
            fixture.componentRef.setInput('loading', false);
            fixture.componentRef.setInput('verificationPassed', true);
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();

            const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
            expect(applyButton.nativeElement.disabled).toBe(false);
        });

        it('should check required ASSET fields correctly', () => {
            const configWithMissingAsset: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyDescriptors: {
                                    file: {
                                        name: 'file',
                                        type: 'ASSET',
                                        required: true
                                    }
                                },
                                propertyValues: {
                                    file: {
                                        valueType: 'ASSET_REFERENCE',
                                        assetReferences: [] // Empty required asset
                                    }
                                }
                            }
                        ]
                    }
                ]
            };
            fixture.componentRef.setInput('workingConfiguration', configWithMissingAsset);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(true);
        });

        it('should pass when required ASSET field has value', () => {
            const configWithAsset: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyDescriptors: {
                                    file: {
                                        name: 'file',
                                        type: 'ASSET',
                                        required: true
                                    }
                                },
                                propertyValues: {
                                    file: {
                                        valueType: 'ASSET_REFERENCE',
                                        assetReferences: [{ id: 'asset-1', name: 'file.txt' }]
                                    }
                                }
                            }
                        ]
                    }
                ]
            };
            fixture.componentRef.setInput('workingConfiguration', configWithAsset);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should return true when required ASSET has reference but missingContent', () => {
            const configWithMissingBlob: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyDescriptors: {
                                    file: {
                                        name: 'file',
                                        type: 'ASSET',
                                        required: true
                                    }
                                },
                                propertyValues: {
                                    file: {
                                        valueType: 'ASSET_REFERENCE',
                                        assetReferences: [{ id: 'asset-1', name: 'gone.jar', missingContent: true }]
                                    }
                                }
                            }
                        ]
                    }
                ]
            };
            fixture.componentRef.setInput('workingConfiguration', configWithMissingBlob);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(true);
        });

        it('should return false when optional ASSET has missingContent only', () => {
            const config: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyDescriptors: {
                                    file: {
                                        name: 'file',
                                        type: 'ASSET',
                                        required: false
                                    }
                                },
                                propertyValues: {
                                    file: {
                                        valueType: 'ASSET_REFERENCE',
                                        assetReferences: [{ id: 'asset-1', name: 'gone.jar', missingContent: true }]
                                    }
                                }
                            }
                        ]
                    }
                ]
            };
            fixture.componentRef.setInput('workingConfiguration', config);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(false);
        });
    });

    describe('Step Visibility', () => {
        const multiStepConfig: ConnectorConfiguration = {
            configurationStepConfigurations: [
                {
                    configurationStepName: 'Step 1',
                    dependencies: [],
                    propertyGroupConfigurations: [
                        {
                            propertyGroupName: 'Group 1',
                            propertyGroupDescription: 'First group',
                            propertyDescriptors: {
                                field1: { name: 'field1', type: 'STRING', required: true }
                            },
                            propertyValues: {
                                field1: { valueType: 'STRING_LITERAL', value: 'value1' }
                            }
                        }
                    ]
                },
                {
                    configurationStepName: 'Step 2',
                    dependencies: [],
                    propertyGroupConfigurations: [
                        {
                            propertyGroupName: 'Group 2',
                            propertyGroupDescription: 'Second group',
                            propertyDescriptors: {
                                field2: { name: 'field2', type: 'STRING', required: true }
                            },
                            propertyValues: {}
                        }
                    ]
                },
                {
                    configurationStepName: 'Step 3',
                    dependencies: [],
                    propertyGroupConfigurations: [
                        {
                            propertyGroupName: 'Group 3',
                            propertyGroupDescription: 'Third group',
                            propertyDescriptors: {
                                field3: { name: 'field3', type: 'STRING', required: true }
                            },
                            propertyValues: {
                                field3: { valueType: 'STRING_LITERAL', value: 'value3' }
                            }
                        }
                    ]
                }
            ]
        };

        it('should return all steps when visibleStepNames is empty', () => {
            fixture.componentRef.setInput('workingConfiguration', multiStepConfig);
            fixture.componentRef.setInput('visibleStepNames', []);
            fixture.detectChanges();

            expect(component.visibleSteps().length).toBe(3);
        });

        it('should filter steps based on visibleStepNames', () => {
            fixture.componentRef.setInput('workingConfiguration', multiStepConfig);
            fixture.componentRef.setInput('visibleStepNames', ['Step 1', 'Step 3']);
            fixture.detectChanges();

            const visible = component.visibleSteps();
            expect(visible.length).toBe(2);
            expect(visible[0].configurationStepName).toBe('Step 1');
            expect(visible[1].configurationStepName).toBe('Step 3');
        });

        it('should preserve order from visibleStepNames', () => {
            fixture.componentRef.setInput('workingConfiguration', multiStepConfig);
            fixture.componentRef.setInput('visibleStepNames', ['Step 3', 'Step 1']);
            fixture.detectChanges();

            const visible = component.visibleSteps();
            expect(visible.length).toBe(2);
            expect(visible[0].configurationStepName).toBe('Step 3');
            expect(visible[1].configurationStepName).toBe('Step 1');
        });

        it('should only validate visible steps for missing required fields', () => {
            // Step 2 has missing required field, but we only show Step 1 and Step 3
            fixture.componentRef.setInput('workingConfiguration', multiStepConfig);
            fixture.componentRef.setInput('visibleStepNames', ['Step 1', 'Step 3']);
            fixture.detectChanges();

            // Should pass because Step 2 (with missing field) is not visible
            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should fail validation when visible step has missing required field', () => {
            // Include Step 2 which has missing required field
            fixture.componentRef.setInput('workingConfiguration', multiStepConfig);
            fixture.componentRef.setInput('visibleStepNames', ['Step 1', 'Step 2']);
            fixture.detectChanges();

            expect(component.hasMissingRequiredFields()).toBe(true);
        });
    });

    describe('Property Visibility in Validation', () => {
        it('should skip hidden required properties in validation', () => {
            const configWithDependencies: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyGroupDescription: 'Test group',
                                propertyDescriptors: {
                                    trigger: { name: 'trigger', type: 'STRING', required: true },
                                    dependent: {
                                        name: 'dependent',
                                        type: 'STRING',
                                        required: true,
                                        dependencies: [{ propertyName: 'trigger', dependentValues: ['yes'] }]
                                    }
                                },
                                propertyValues: {
                                    trigger: { valueType: 'STRING_LITERAL', value: 'no' }
                                    // dependent has no value, but trigger='no' so it should be hidden
                                }
                            }
                        ]
                    }
                ]
            };

            fixture.componentRef.setInput('workingConfiguration', configWithDependencies);
            fixture.detectChanges();

            // Should pass because 'dependent' is hidden (trigger != 'yes')
            expect(component.hasMissingRequiredFields()).toBe(false);
        });

        it('should validate visible required properties', () => {
            const configWithDependencies: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyGroupDescription: 'Test group',
                                propertyDescriptors: {
                                    trigger: { name: 'trigger', type: 'STRING', required: true },
                                    dependent: {
                                        name: 'dependent',
                                        type: 'STRING',
                                        required: true,
                                        dependencies: [{ propertyName: 'trigger', dependentValues: ['yes'] }]
                                    }
                                },
                                propertyValues: {
                                    trigger: { valueType: 'STRING_LITERAL', value: 'yes' }
                                    // dependent has no value, and trigger='yes' so it IS visible
                                }
                            }
                        ]
                    }
                ]
            };

            fixture.componentRef.setInput('workingConfiguration', configWithDependencies);
            fixture.detectChanges();

            // Should fail because 'dependent' is visible and has no value
            expect(component.hasMissingRequiredFields()).toBe(true);
        });
    });

    describe('filteredVisibleSteps', () => {
        it('should return empty array when no configuration', () => {
            fixture.componentRef.setInput('workingConfiguration', null);
            fixture.detectChanges();

            expect(component.filteredVisibleSteps()).toEqual([]);
        });

        it('should filter out hidden properties from groups', () => {
            const configWithDependencies: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyGroupDescription: 'Test group',
                                propertyDescriptors: {
                                    trigger: { name: 'trigger', type: 'STRING', required: true },
                                    dependent: {
                                        name: 'dependent',
                                        type: 'STRING',
                                        required: true,
                                        dependencies: [{ propertyName: 'trigger', dependentValues: ['yes'] }]
                                    }
                                },
                                propertyValues: {
                                    trigger: { valueType: 'STRING_LITERAL', value: 'no' }
                                }
                            }
                        ]
                    }
                ]
            };

            fixture.componentRef.setInput('workingConfiguration', configWithDependencies);
            fixture.detectChanges();

            const filteredSteps = component.filteredVisibleSteps();
            const filteredGroup = filteredSteps[0].propertyGroupConfigurations[0];

            expect(Object.keys(filteredGroup.propertyDescriptors)).toContain('trigger');
            expect(Object.keys(filteredGroup.propertyDescriptors)).not.toContain('dependent');
        });

        it('should include visible dependent properties', () => {
            const configWithDependencies: ConnectorConfiguration = {
                configurationStepConfigurations: [
                    {
                        configurationStepName: 'Step 1',
                        dependencies: [],
                        propertyGroupConfigurations: [
                            {
                                propertyGroupName: 'Group 1',
                                propertyGroupDescription: 'Test group',
                                propertyDescriptors: {
                                    trigger: { name: 'trigger', type: 'STRING', required: true },
                                    dependent: {
                                        name: 'dependent',
                                        type: 'STRING',
                                        required: true,
                                        dependencies: [{ propertyName: 'trigger', dependentValues: ['yes'] }]
                                    }
                                },
                                propertyValues: {
                                    trigger: { valueType: 'STRING_LITERAL', value: 'yes' }
                                }
                            }
                        ]
                    }
                ]
            };

            fixture.componentRef.setInput('workingConfiguration', configWithDependencies);
            fixture.detectChanges();

            const filteredSteps = component.filteredVisibleSteps();
            const filteredGroup = filteredSteps[0].propertyGroupConfigurations[0];

            expect(Object.keys(filteredGroup.propertyDescriptors)).toContain('trigger');
            expect(Object.keys(filteredGroup.propertyDescriptors)).toContain('dependent');
        });
    });

    describe('Verify-all-steps functionality', () => {
        const sourceStepName = 'Source Database Configuration';
        const destinationStepName = 'Destination Database Configuration';

        const mockVerificationError: ConfigVerificationResult = {
            outcome: 'FAILED',
            verificationStepName: 'test-step',
            explanation: 'Connection failed'
        };

        describe('Verify button', () => {
            beforeEach(() => {
                fixture.componentRef.setInput('loading', false);
                fixture.componentRef.setInput('applying', false);
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
                fixture.detectChanges();
            });

            it('should render a verify button', () => {
                const verifyButton = fixture.debugElement.query(By.css('[data-qa="verify-button"]'));
                expect(verifyButton).toBeTruthy();
            });

            it('should emit verify output when clicked', () => {
                const verifySpy = vi.spyOn(component.verify, 'emit');
                const verifyButton = fixture.debugElement.query(By.css('[data-qa="verify-button"]'));

                verifyButton.nativeElement.click();

                expect(verifySpy).toHaveBeenCalledTimes(1);
            });

            it('should disable verify button when verifying is true', () => {
                fixture.componentRef.setInput('verifying', true);
                fixture.detectChanges();

                const verifyButton = fixture.debugElement.query(By.css('[data-qa="verify-button"]'));
                expect(verifyButton.nativeElement.disabled).toBe(true);
            });

            it('should show spinner in verify button when verifying', () => {
                fixture.componentRef.setInput('verifying', true);
                fixture.detectChanges();

                const verifyButton = fixture.debugElement.query(By.css('[data-qa="verify-button"]'));
                expect(verifyButton).toBeTruthy();
                const spans = verifyButton.queryAll(By.css('span'));
                expect(spans.length).toBeGreaterThan(0);
            });
        });

        describe('Apply button gate', () => {
            beforeEach(() => {
                fixture.componentRef.setInput('loading', false);
                fixture.componentRef.setInput('applying', false);
                fixture.componentRef.setInput('verifying', false);
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            });

            it('should disable apply when verificationPassed is null (not yet verified)', () => {
                fixture.componentRef.setInput('verificationPassed', null);
                fixture.detectChanges();

                const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
                expect(applyButton.nativeElement.disabled).toBe(true);
            });

            it('should disable apply when verificationPassed is false', () => {
                fixture.componentRef.setInput('verificationPassed', false);
                fixture.detectChanges();

                const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
                expect(applyButton.nativeElement.disabled).toBe(true);
            });

            it('should enable apply when verificationPassed is true', () => {
                fixture.componentRef.setInput('verificationPassed', true);
                fixture.detectChanges();

                const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
                expect(applyButton.nativeElement.disabled).toBe(false);
            });

            it('should disable apply when verifying is true', () => {
                fixture.componentRef.setInput('verificationPassed', true);
                fixture.componentRef.setInput('verifying', true);
                fixture.detectChanges();

                const applyButton = fixture.debugElement.query(By.css('[data-qa="apply-button"]'));
                expect(applyButton.nativeElement.disabled).toBe(true);
            });
        });

        describe('Step verification status icons', () => {
            beforeEach(() => {
                fixture.componentRef.setInput('loading', false);
                fixture.componentRef.setInput('applying', false);
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            });

            it('should not show inline spinner for step being verified (dialog is used instead)', () => {
                fixture.componentRef.setInput('verifying', true);
                fixture.componentRef.setInput('currentVerifyingStepName', sourceStepName);
                fixture.detectChanges();

                const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
                const sourceSection = stepSections[0];
                expect(sourceSection.query(By.css('[data-qa="step-verifying-spinner"]'))).toBeFalsy();
                expect(sourceSection.query(By.css('[data-qa="step-passed-icon"]'))).toBeFalsy();
                expect(sourceSection.query(By.css('[data-qa="step-failed-icon"]'))).toBeFalsy();
            });

            it('should show check-circle icon for passed step', () => {
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: []
                });
                fixture.detectChanges();

                const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
                const sourceSection = stepSections[0];
                const passedIcon = sourceSection.query(By.css('[data-qa="step-passed-icon"]'));
                expect(passedIcon).toBeTruthy();
                expect(passedIcon.nativeElement.classList).toContain('fa-check-circle');
            });

            it('should show error-circle icon for failed step', () => {
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [mockVerificationError]
                });
                fixture.detectChanges();

                const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
                const sourceSection = stepSections[0];
                const failedIcon = sourceSection.query(By.css('[data-qa="step-failed-icon"]'));
                expect(failedIcon).toBeTruthy();
                expect(failedIcon.nativeElement.classList).toContain('fa-times-circle');
            });

            it('should not show icon when step has no verification status', () => {
                fixture.componentRef.setInput('verifying', false);
                fixture.componentRef.setInput('currentVerifyingStepName', null);
                fixture.componentRef.setInput('stepVerificationResults', {});
                fixture.detectChanges();

                const stepSections = fixture.debugElement.queryAll(By.css('[data-qa="step-section"]'));
                const destinationSection = stepSections[1];
                expect(destinationSection.query(By.css('[data-qa="step-verifying-spinner"]'))).toBeFalsy();
                expect(destinationSection.query(By.css('[data-qa="step-passed-icon"]'))).toBeFalsy();
                expect(destinationSection.query(By.css('[data-qa="step-failed-icon"]'))).toBeFalsy();
            });
        });

        describe('getStepVerificationStatus method', () => {
            it("should return 'verifying' when step is currently being verified", () => {
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
                fixture.componentRef.setInput('verifying', true);
                fixture.componentRef.setInput('currentVerifyingStepName', sourceStepName);
                fixture.detectChanges();

                expect(component.getStepVerificationStatus(sourceStepName)).toBe('verifying');
            });

            it("should return 'passed' when stepVerificationResults has empty array", () => {
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
                fixture.componentRef.setInput('verifying', false);
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: []
                });
                fixture.detectChanges();

                expect(component.getStepVerificationStatus(sourceStepName)).toBe('passed');
            });

            it("should return 'failed' when stepVerificationResults has error entries", () => {
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [mockVerificationError]
                });
                fixture.detectChanges();

                expect(component.getStepVerificationStatus(sourceStepName)).toBe('failed');
            });

            it('should return null when step has no results and verification has not run', () => {
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
                fixture.componentRef.setInput('stepVerificationResults', {});
                fixture.detectChanges();

                expect(component.getStepVerificationStatus(destinationStepName)).toBeNull();
            });

            it("should return 'skipped' when verification completed but step has no results", () => {
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
                fixture.componentRef.setInput('verificationPassed', false);
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'check',
                            explanation: 'Failed'
                        }
                    ]
                });
                fixture.detectChanges();

                expect(component.getStepVerificationStatus(destinationStepName)).toBe('skipped');
            });
        });

        describe('getGeneralErrorMessages method', () => {
            beforeEach(() => {
                fixture.componentRef.setInput('loading', false);
                fixture.componentRef.setInput('applying', false);
                fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            });

            it('should return errors without a subject', () => {
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'general-check',
                            explanation: 'General failure'
                        }
                    ]
                });
                fixture.detectChanges();

                const messages = component.getGeneralErrorMessages(sourceStepName);
                expect(messages).toEqual(['General failure']);
            });

            it('should treat errors with unmatched subjects as general errors', () => {
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'field-check',
                            subject: 'nonexistent-property',
                            explanation: 'Unknown property error'
                        }
                    ]
                });
                fixture.detectChanges();

                const messages = component.getGeneralErrorMessages(sourceStepName);
                expect(messages).toEqual(['Unknown property error']);
            });

            it('should not include matched subject errors in general messages', () => {
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'host-check',
                            subject: 'hostname',
                            explanation: 'Host unreachable'
                        }
                    ]
                });
                fixture.detectChanges();

                const messages = component.getGeneralErrorMessages(sourceStepName);
                expect(messages).toEqual([]);
            });

            it('should separate matched subject errors from general and unmatched errors', () => {
                fixture.componentRef.setInput('stepVerificationResults', {
                    [sourceStepName]: [
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'host-check',
                            subject: 'hostname',
                            explanation: 'Host unreachable'
                        },
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'general-check',
                            explanation: 'Plugin issue'
                        },
                        {
                            outcome: 'FAILED',
                            verificationStepName: 'unknown-check',
                            subject: 'missing-field',
                            explanation: 'Missing field error'
                        }
                    ]
                });
                fixture.detectChanges();

                const messages = component.getGeneralErrorMessages(sourceStepName);
                expect(messages).toEqual(['Plugin issue', 'Missing field error']);
            });
        });
    });

    describe('Verify All Error Banner', () => {
        beforeEach(() => {
            fixture.componentRef.setInput('workingConfiguration', mockConfiguration);
            fixture.detectChanges();
        });

        it('should not display error banner when verifyAllError is null', () => {
            fixture.componentRef.setInput('verifyAllError', null);
            fixture.detectChanges();

            const banner = fixture.debugElement.query(By.css('[data-qa="verify-all-error-banner"]'));
            expect(banner).toBeNull();
        });

        it('should display error banner when verifyAllError is set', () => {
            fixture.componentRef.setInput('verifyAllError', 'Network request failed');
            fixture.detectChanges();

            const banner = fixture.debugElement.query(By.css('[data-qa="verify-all-error-banner"]'));
            expect(banner).toBeTruthy();
        });
    });
});
