/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { PropertyGroupCard } from './property-group-card.component';
import { ConfigVerificationResult, PropertyGroupConfiguration } from '../../types';

describe('PropertyGroupCard', () => {
    function makeGroup(overrides: Partial<PropertyGroupConfiguration> = {}): PropertyGroupConfiguration {
        return {
            propertyGroupName: 'Connection',
            propertyDescriptors: {
                Host: { name: 'Host', type: 'STRING', required: true },
                Port: { name: 'Port', type: 'STRING', required: false }
            },
            propertyValues: {
                Host: { value: 'localhost', valueType: 'STRING_LITERAL' },
                Port: { value: '5432', valueType: 'STRING_LITERAL' }
            },
            ...overrides
        };
    }

    interface SetupOptions {
        propertyGroup?: PropertyGroupConfiguration;
        hideGroupName?: boolean;
        verificationErrors?: ConfigVerificationResult[];
    }

    async function setup(options: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            imports: [PropertyGroupCard, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(PropertyGroupCard);
        const component = fixture.componentInstance;

        fixture.componentRef.setInput('propertyGroup', options.propertyGroup ?? makeGroup());
        if (options.hideGroupName !== undefined) fixture.componentRef.setInput('hideGroupName', options.hideGroupName);
        if (options.verificationErrors !== undefined)
            fixture.componentRef.setInput('verificationErrors', options.verificationErrors);

        fixture.detectChanges();

        const queryAll = (qa: string) => fixture.debugElement.queryAll(By.css(`[data-qa="${qa}"]`));
        const query = (qa: string) => fixture.debugElement.query(By.css(qa));

        return { fixture, component, queryAll, query };
    }

    describe('creation', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });
    });

    describe('group name header', () => {
        it('should display the group name by default', async () => {
            const { query } = await setup();
            const title = query('mat-card-title');
            expect(title).toBeTruthy();
            expect(title.nativeElement.textContent.trim()).toBe('Connection');
        });

        it('should hide the group name when hideGroupName is true', async () => {
            const { query } = await setup({ hideGroupName: true });
            expect(query('mat-card-title')).toBeFalsy();
        });
    });

    describe('getPropertyNames', () => {
        it('should return all property names from descriptors', async () => {
            const { component } = await setup();
            expect(component.getPropertyNames()).toEqual(['Host', 'Port']);
        });

        it('should return empty array when no descriptors exist', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({ propertyDescriptors: {} })
            });
            expect(component.getPropertyNames()).toEqual([]);
        });
    });

    describe('getDescriptor', () => {
        it('should return the descriptor for a known property', async () => {
            const { component } = await setup();
            const descriptor = component.getDescriptor('Host');
            expect(descriptor).toBeDefined();
            expect(descriptor!.name).toBe('Host');
            expect(descriptor!.required).toBe(true);
        });

        it('should return undefined for an unknown property', async () => {
            const { component } = await setup();
            expect(component.getDescriptor('NonExistent')).toBeUndefined();
        });
    });

    describe('isRequired', () => {
        it('should return true for required properties', async () => {
            const { component } = await setup();
            expect(component.isRequired('Host')).toBe(true);
        });

        it('should return false for optional properties', async () => {
            const { component } = await setup();
            expect(component.isRequired('Port')).toBe(false);
        });

        it('should return false for unknown properties', async () => {
            const { component } = await setup();
            expect(component.isRequired('NonExistent')).toBe(false);
        });
    });

    describe('hasValue', () => {
        it('should return true for STRING_LITERAL with a value', async () => {
            const { component } = await setup();
            expect(component.hasValue('Host')).toBe(true);
        });

        it('should return false when no value reference exists', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {}
                })
            });
            expect(component.hasValue('Host')).toBe(false);
        });

        it('should return false for STRING_LITERAL with null value', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: { value: null, valueType: 'STRING_LITERAL' }
                    }
                })
            });
            expect(component.hasValue('Host')).toBe(false);
        });

        it('should return false for STRING_LITERAL with empty string', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: { value: '', valueType: 'STRING_LITERAL' }
                    }
                })
            });
            expect(component.hasValue('Host')).toBe(false);
        });

        it('should return true for SECRET_REFERENCE regardless of value', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: { valueType: 'SECRET_REFERENCE' }
                    }
                })
            });
            expect(component.hasValue('Host')).toBe(true);
        });

        it('should return true for ASSET_REFERENCE with entries', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: {
                            valueType: 'ASSET_REFERENCE',
                            assetReferences: [{ id: 'asset-1', name: 'cert.pem' }]
                        }
                    }
                })
            });
            expect(component.hasValue('Host')).toBe(true);
        });

        it('should return false for ASSET_REFERENCE with empty array', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: { valueType: 'ASSET_REFERENCE', assetReferences: [] }
                    }
                })
            });
            expect(component.hasValue('Host')).toBe(false);
        });
    });

    describe('getDisplayValueForProperty', () => {
        it('should return the string value for STRING_LITERAL', async () => {
            const { component } = await setup();
            expect(component.getDisplayValueForProperty('Host')).toBe('localhost');
        });

        it('should return masked text for SECRET_REFERENCE', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: { valueType: 'SECRET_REFERENCE' }
                    }
                })
            });
            expect(component.getDisplayValueForProperty('Host')).toBe('••••••••');
        });

        it('should return comma-separated asset names for ASSET_REFERENCE', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: {
                            valueType: 'ASSET_REFERENCE',
                            assetReferences: [
                                { id: 'a1', name: 'cert.pem' },
                                { id: 'a2', name: 'key.pem' }
                            ]
                        }
                    }
                })
            });
            expect(component.getDisplayValueForProperty('Host')).toBe('cert.pem, key.pem');
        });

        it('should fall back to asset id when name is missing', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({
                    propertyValues: {
                        Host: {
                            valueType: 'ASSET_REFERENCE',
                            assetReferences: [{ id: 'a1' }]
                        }
                    }
                })
            });
            expect(component.getDisplayValueForProperty('Host')).toBe('a1');
        });

        it('should return empty string when no value reference exists', async () => {
            const { component } = await setup({
                propertyGroup: makeGroup({ propertyValues: {} })
            });
            expect(component.getDisplayValueForProperty('Host')).toBe('');
        });
    });

    describe('fieldErrors computed signal', () => {
        it('should return empty object when no verification errors', async () => {
            const { component } = await setup();
            expect(component.fieldErrors()).toEqual({});
        });

        it('should map errors to matching property names', async () => {
            const { component } = await setup({
                verificationErrors: [
                    { outcome: 'FAILED', verificationStepName: 'Check', subject: 'Host', explanation: 'Invalid host' }
                ]
            });
            expect(component.fieldErrors()).toEqual({ Host: 'Invalid host' });
        });

        it('should ignore errors whose subject is not in the property group', async () => {
            const { component } = await setup({
                verificationErrors: [
                    {
                        outcome: 'FAILED',
                        verificationStepName: 'Check',
                        subject: 'UnknownProp',
                        explanation: 'Bad value'
                    }
                ]
            });
            expect(component.fieldErrors()).toEqual({});
        });

        it('should ignore errors without a subject', async () => {
            const { component } = await setup({
                verificationErrors: [
                    { outcome: 'FAILED', verificationStepName: 'Check', explanation: 'General failure' }
                ]
            });
            expect(component.fieldErrors()).toEqual({});
        });
    });

    describe('getFieldError', () => {
        it('should return the error message for a property with a field error', async () => {
            const { component } = await setup({
                verificationErrors: [
                    {
                        outcome: 'FAILED',
                        verificationStepName: 'Check',
                        subject: 'Port',
                        explanation: 'Port out of range'
                    }
                ]
            });
            expect(component.getFieldError('Port')).toBe('Port out of range');
        });

        it('should return null for a property without a field error', async () => {
            const { component } = await setup();
            expect(component.getFieldError('Host')).toBeNull();
        });
    });

    describe('template rendering', () => {
        it('should render a row for each property', async () => {
            const { query } = await setup();
            const labels = query('mat-card-content').queryAll(By.css('mat-label'));
            expect(labels.length).toBe(2);
            const texts = labels.map((el) => el.nativeElement.textContent.trim());
            expect(texts).toContain('Host');
            expect(texts).toContain('Port');
        });

        it('should show required asterisk for required properties', async () => {
            const { query } = await setup();
            const content = query('mat-card-content');
            const asterisks = content.queryAll(By.css('.error-color'));
            expect(asterisks.length).toBeGreaterThanOrEqual(1);
        });

        it('should show "No value set" for properties without values', async () => {
            const { query } = await setup({
                propertyGroup: makeGroup({ propertyValues: {} })
            });
            const unsetLabels = query('mat-card-content').queryAll(By.css('.unset'));
            expect(unsetLabels.length).toBe(2);
            expect(unsetLabels[0].nativeElement.textContent.trim()).toBe('No value set');
        });

        it('should display the property value for properties with values', async () => {
            const { query } = await setup();
            const content = query('mat-card-content');
            const valueSpans = content.queryAll(By.css('.tertiary-color'));
            const texts = valueSpans.map((el) => el.nativeElement.textContent.trim());
            expect(texts).toContain('localhost');
            expect(texts).toContain('5432');
        });

        it('should render mat-error for properties with verification errors', async () => {
            const { query } = await setup({
                verificationErrors: [
                    { outcome: 'FAILED', verificationStepName: 'Check', subject: 'Host', explanation: 'Cannot resolve' }
                ]
            });
            const errors = query('mat-card-content').queryAll(By.css('mat-error'));
            expect(errors.length).toBe(1);
            expect(errors[0].nativeElement.textContent.trim()).toBe('Cannot resolve');
        });

        it('should not render mat-error when there are no verification errors', async () => {
            const { query } = await setup();
            const errors = query('mat-card-content').queryAll(By.css('mat-error'));
            expect(errors.length).toBe(0);
        });
    });
});
