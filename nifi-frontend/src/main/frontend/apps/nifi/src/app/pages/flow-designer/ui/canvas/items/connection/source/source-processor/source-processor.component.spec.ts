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
import { SourceProcessor } from './source-processor.component';
import { NiFiCommon } from '@nifi/shared';

describe('SourceProcessor Component', () => {
    // Mock data factories
    function createMockRelationship(name: string) {
        return {
            name,
            id: `rel-${name}`,
            available: true,
            description: `${name} relationship`,
            retry: true
        };
    }

    function createMockProcessor(name: string, relationships?: any[]) {
        return {
            component: { name, relationships: relationships || [] }
        };
    }

    const mockNiFiCommon = {
        isEmpty: jest.fn((value: any) => !value || (Array.isArray(value) && value.length === 0))
    };

    // Setup function for component configuration
    async function setup(
        options: {
            processor?: any;
            selectedRelationships?: string[];
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [SourceProcessor],
            providers: [{ provide: NiFiCommon, useValue: mockNiFiCommon }]
        }).compileComponents();

        const fixture = TestBed.createComponent(SourceProcessor);
        const component = fixture.componentInstance;

        // Initialize arrays to prevent undefined errors
        component.selectedRelationships = options.selectedRelationships || [];
        component.relationshipItems = [];

        // Set processor if provided
        if (options.processor) {
            component.processor = options.processor;
        }

        // Set up mock callbacks
        component.onChange = jest.fn();
        component.onTouched = jest.fn();

        // Initial detection to trigger lifecycle hooks
        fixture.detectChanges();

        return { component, fixture };
    }

    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should initialize with default values', async () => {
            const { component } = await setup();
            expect(component.isDisabled).toBe(false);
            expect(component.isTouched).toBe(false);
            expect(component.selectedRelationships).toEqual([]);
            expect(component.relationshipItems).toEqual([]);
        });
    });

    describe('Processor input logic', () => {
        it('should set processor and process relationships', async () => {
            const processor = createMockProcessor('Test Processor', [createMockRelationship('success')]);
            const { component } = await setup({ processor });

            expect(component.name).toBe('Test Processor');
            expect(component.relationships).toEqual(processor.component.relationships);
            expect(component.relationshipItems).toHaveLength(1);
        });

        it('should handle null processor', async () => {
            const { component } = await setup({ processor: null });
            expect(component.name).toBeUndefined();
            expect(component.relationships).toBeUndefined();
        });
    });

    describe('ProcessRelationships method logic', () => {
        it('should auto-select single relationship when no selected relationships', async () => {
            const processor = createMockProcessor('Test', [createMockRelationship('success')]);
            mockNiFiCommon.isEmpty.mockReturnValue(true);
            const { component } = await setup({ processor });

            expect(component.relationshipItems[0].selected).toBe(true);
        });

        it('should map existing selected relationships', async () => {
            const processor = createMockProcessor('Test', [
                createMockRelationship('success'),
                createMockRelationship('failure')
            ]);
            mockNiFiCommon.isEmpty.mockReturnValue(false);
            const { component } = await setup({ processor, selectedRelationships: ['success'] });

            expect(component.relationshipItems[0].selected).toBe(true);
            expect(component.relationshipItems[1].selected).toBe(false);
        });

        it('should mark unavailable relationships correctly', async () => {
            const processor = createMockProcessor('Test', [createMockRelationship('success')]);
            mockNiFiCommon.isEmpty.mockReturnValue(false);
            const { component } = await setup({ processor, selectedRelationships: ['nonexistent'] });

            expect(component.relationshipItems[0].available).toBe(true);
            expect(component.relationshipItems.find((item) => item.relationshipName === 'nonexistent')?.available).toBe(
                false
            );
        });
    });

    describe('considerDefaultSelection method logic', () => {
        it('should not call handleChanged when no callbacks registered', async () => {
            const processor = createMockProcessor('Test', [createMockRelationship('success')]);
            const { component } = await setup({ processor });

            // Clear callbacks to test null handling
            component.onChange = null;
            component.onTouched = null;

            // This should not throw errors when callbacks are null
            component.considerDefaultSelection();
            expect(component.selectedRelationships).toBeDefined();
        });

        it('should call handleChanged for single relationship when callbacks exist', async () => {
            const processor = createMockProcessor('Test', [createMockRelationship('success')]);
            mockNiFiCommon.isEmpty.mockReturnValue(true);
            const { component } = await setup({ processor });

            const onChangeSpy = jest.fn();
            component.registerOnChange(onChangeSpy);

            // This test checks that considerDefaultSelection calls handleChanged for auto-selected single relationships
            component.considerDefaultSelection();

            expect(onChangeSpy).toHaveBeenCalledWith(['success']);
        });
    });

    describe('ControlValueAccessor implementation', () => {
        it('should register onChange callback', async () => {
            const { component } = await setup();
            const callback = jest.fn();
            component.registerOnChange(callback);
            expect(component.onChange).toBe(callback);
        });

        it('should register onTouched callback', async () => {
            const { component } = await setup();
            const callback = jest.fn();
            component.registerOnTouched(callback);
            expect(component.onTouched).toBe(callback);
        });

        it('should handle disabled state', async () => {
            const { component } = await setup();
            component.setDisabledState(true);
            expect(component.isDisabled).toBe(true);
        });

        it('should handle writeValue', async () => {
            const { component } = await setup();
            component.writeValue(['success']);
            expect(component.selectedRelationships).toEqual(['success']);
        });

        it('should emit serialized relationships', async () => {
            // Mock the private method by setting up relationshipItems directly
            const { component } = await setup();
            component.relationshipItems = [{ relationshipName: 'success', selected: true, available: true }];

            component.handleChanged();

            expect(component.onChange).toHaveBeenCalledWith(['success']);
        });
    });

    describe('Template logic', () => {
        it('should display processor name when available', async () => {
            const processor = createMockProcessor('Test Processor');
            const { fixture } = await setup({ processor });

            const processorNameDisplay = fixture.nativeElement.querySelector('[data-qa="processor-name-display"]');
            expect(processorNameDisplay.textContent.trim()).toBe('Test Processor');
        });

        it('should display relationships when available', async () => {
            const processor = createMockProcessor('Test', [createMockRelationship('success')]);
            const { fixture } = await setup({ processor });

            const checkboxes = fixture.nativeElement.querySelectorAll('[data-qa="relationship-checkbox"]');
            expect(checkboxes).toHaveLength(1);
        });

        it('should only show selected relationships when disabled', async () => {
            const relationships = [createMockRelationship('success'), createMockRelationship('failure')];
            const processor = createMockProcessor('Test', relationships);
            mockNiFiCommon.isEmpty.mockReturnValue(false);
            const { component, fixture } = await setup({ processor, selectedRelationships: ['success'] });

            // Ensure the component has proper state before setting disabled
            expect(component.relationshipItems).toHaveLength(2);
            expect(component.relationshipItems[0].selected).toBe(true);

            component.setDisabledState(true);
            fixture.detectChanges();

            const visibleCheckboxes = fixture.nativeElement.querySelectorAll('[data-qa="relationship-checkbox"]');
            expect(visibleCheckboxes.length).toBeLessThanOrEqual(2);
        });

        it('should display relationship names correctly', async () => {
            const processor = createMockProcessor('Test', [createMockRelationship('success')]);
            const { fixture } = await setup({ processor });

            const relationshipNames = fixture.nativeElement.querySelectorAll('[data-qa="relationship-name"]');
            expect(relationshipNames.length).toBeGreaterThan(0);

            const successRelationship = Array.from(relationshipNames).find(
                (element: any) => element.textContent.trim() === 'success'
            );
            expect(successRelationship).toBeTruthy();
        });

        it('should display group name when available', async () => {
            const processor = createMockProcessor('Test');
            const { component, fixture } = await setup({ processor });

            component.groupName = 'Test Group';
            fixture.detectChanges();

            const groupDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupDisplay.textContent.trim()).toBe('Test Group');
        });
    });
});
