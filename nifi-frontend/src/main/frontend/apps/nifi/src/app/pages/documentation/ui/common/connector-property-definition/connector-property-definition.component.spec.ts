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

import { ConnectorPropertyDefinitionComponent } from './connector-property-definition.component';
import { ConnectorPropertyDescriptor } from '../../../state/connector-definition';

describe('ConnectorPropertyDefinitionComponent', () => {
    function createMockProperty(overrides?: Partial<ConnectorPropertyDescriptor>): ConnectorPropertyDescriptor {
        return {
            name: 'Test Property',
            description: 'A test property description',
            required: false,
            allowableValuesFetchable: false,
            ...overrides
        };
    }

    async function setup(propertyOverrides?: Partial<ConnectorPropertyDescriptor>) {
        await TestBed.configureTestingModule({
            imports: [ConnectorPropertyDefinitionComponent]
        }).compileComponents();

        const fixture = TestBed.createComponent(ConnectorPropertyDefinitionComponent);
        const component = fixture.componentInstance;
        component.propertyDescriptor = createMockProperty(propertyOverrides);
        fixture.detectChanges();

        return { component, fixture };
    }

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should have property descriptor set', async () => {
            const { component } = await setup({ name: 'Custom Property' });
            expect(component.propertyDescriptor.name).toBe('Custom Property');
        });
    });

    describe('Default value formatting', () => {
        it('should return undefined when no default value is set', async () => {
            const { component } = await setup({ defaultValue: undefined });
            expect(component.formatDefaultValue()).toBeUndefined();
        });

        it('should return the default value as-is when no allowable values exist', async () => {
            const { component } = await setup({ defaultValue: 'my-default' });
            expect(component.formatDefaultValue()).toBe('my-default');
        });

        it('should return the display name when default value matches an allowable value', async () => {
            const { component } = await setup({
                defaultValue: 'val1',
                allowableValues: [
                    { value: 'val1', displayName: 'Value One', description: 'First value' },
                    { value: 'val2', displayName: 'Value Two', description: 'Second value' }
                ]
            });
            expect(component.formatDefaultValue()).toBe('Value One');
        });

        it('should return the raw default value when it does not match any allowable value', async () => {
            const { component } = await setup({
                defaultValue: 'unknown',
                allowableValues: [{ value: 'val1', displayName: 'Value One', description: 'First value' }]
            });
            expect(component.formatDefaultValue()).toBe('unknown');
        });
    });

    describe('Property type formatting', () => {
        it('should return STRING when property type is undefined', async () => {
            const { component } = await setup({ propertyType: undefined });
            expect(component.formatPropertyType()).toBe('STRING');
        });

        it('should return the property type when defined', async () => {
            const { component } = await setup({ propertyType: 'INTEGER' });
            expect(component.formatPropertyType()).toBe('INTEGER');
        });
    });

    describe('Dependency sorting', () => {
        it('should sort dependencies alphabetically by property name', async () => {
            const { component } = await setup();
            const dependencies = [
                { propertyName: 'zeta', dependentValues: ['a'] },
                { propertyName: 'alpha', dependentValues: ['b'] },
                { propertyName: 'gamma', dependentValues: ['c'] }
            ];

            const sorted = component.sortDependencies(dependencies);

            expect(sorted[0].propertyName).toBe('alpha');
            expect(sorted[1].propertyName).toBe('gamma');
            expect(sorted[2].propertyName).toBe('zeta');
        });

        it('should not mutate the original array', async () => {
            const { component } = await setup();
            const dependencies = [
                { propertyName: 'zeta', dependentValues: ['a'] },
                { propertyName: 'alpha', dependentValues: ['b'] }
            ];

            component.sortDependencies(dependencies);

            expect(dependencies[0].propertyName).toBe('zeta');
        });
    });

    describe('Dependent value formatting', () => {
        it('should return empty string when dependentValues is undefined', async () => {
            const { component } = await setup();
            const dependency = { propertyName: 'prop1' };

            expect(component.formatDependentValue(dependency)).toBe('');
        });

        it('should join dependent values with comma', async () => {
            const { component } = await setup();
            const dependency = { propertyName: 'prop1', dependentValues: ['val1', 'val2', 'val3'] };

            expect(component.formatDependentValue(dependency)).toBe('val1, val2, val3');
        });

        it('should use display names from lookupProperty when available', async () => {
            const { component } = await setup();
            const properties: ConnectorPropertyDescriptor[] = [
                {
                    name: 'dep-prop',
                    description: 'Dependency property',
                    required: false,
                    allowableValuesFetchable: false,
                    allowableValues: [
                        { value: 'v1', displayName: 'Display Value 1', description: '' },
                        { value: 'v2', displayName: 'Display Value 2', description: '' }
                    ]
                }
            ];
            component.lookupProperty = (name: string) => properties.find((p) => p.name === name);

            const dependency = { propertyName: 'dep-prop', dependentValues: ['v1', 'v2'] };

            expect(component.formatDependentValue(dependency)).toBe('Display Value 1, Display Value 2');
        });

        it('should fall back to raw values when lookupProperty does not find the property', async () => {
            const { component } = await setup();
            component.lookupProperty = () => undefined;

            const dependency = { propertyName: 'unknown-prop', dependentValues: ['raw1', 'raw2'] };

            expect(component.formatDependentValue(dependency)).toBe('raw1, raw2');
        });
    });
});
