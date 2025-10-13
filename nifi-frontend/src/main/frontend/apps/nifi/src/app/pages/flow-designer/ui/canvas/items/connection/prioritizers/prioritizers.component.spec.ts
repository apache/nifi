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
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

import { Prioritizers } from './prioritizers.component';
import { NiFiCommon } from '@nifi/shared';
import { DocumentedType } from '../../../../../../../state/shared';

describe('Prioritizers', () => {
    // Mock data factories
    function createDocumentedType(type: string, options: any = {}): DocumentedType {
        return {
            type,
            bundle: {
                group: options.bundleGroup || 'org.apache.nifi',
                artifact: options.bundleArtifact || 'nifi-framework-nar',
                version: options.bundleVersion || '2.0.0-SNAPSHOT'
            },
            description: options.description || '',
            restricted: options.restricted || false,
            tags: options.tags || []
        };
    }

    function createAllPrioritizers(): DocumentedType[] {
        return [
            createDocumentedType('org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer', {
                description: 'A simple First-In-First-Out (FIFO) prioritizer'
            }),
            createDocumentedType('org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer', {
                description: 'Prioritizes FlowFiles based on newest first'
            }),
            createDocumentedType('org.apache.nifi.prioritizer.OldestFlowFileFirstPrioritizer', {
                description: 'Prioritizes FlowFiles based on oldest first'
            })
        ];
    }

    const mockNiFiCommon = {
        getComponentTypeLabel: jest.fn((type: string) => {
            const parts = type.split('.');
            return parts[parts.length - 1];
        }),
        isBlank: jest.fn((value: string) => !value || value.trim() === '')
    };

    // Setup function for component configuration
    async function setup(
        options: {
            allPrioritizers?: DocumentedType[];
            selectedPrioritizers?: string[];
            disabled?: boolean;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [Prioritizers, NoopAnimationsModule],
            providers: [{ provide: NiFiCommon, useValue: mockNiFiCommon }]
        }).compileComponents();

        const fixture = TestBed.createComponent(Prioritizers);
        const component = fixture.componentInstance;

        // Set up mock callbacks
        component.onChange = jest.fn();
        component.onTouched = jest.fn();

        // Initialize with empty value first
        component.writeValue(options.selectedPrioritizers || []);

        // Set allPrioritizers if provided
        if (options.allPrioritizers) {
            component.allPrioritizers = options.allPrioritizers;
        }

        // Set disabled state if provided
        if (options.disabled) {
            component.setDisabledState(options.disabled);
        }

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
            expect(component.availablePrioritizers).toEqual([]);
            expect(component.selectedPrioritizers).toEqual([]);
        });
    });

    describe('AllPrioritizers input setter logic', () => {
        it('should set allPrioritizers and process them', async () => {
            const allPrioritizers = createAllPrioritizers();
            const { component } = await setup({ allPrioritizers });

            expect(component.availablePrioritizers).toEqual(allPrioritizers);
            expect(component.selectedPrioritizers).toEqual([]);
        });

        it('should handle selected prioritizers correctly', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { component } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            expect(component.selectedPrioritizers).toHaveLength(1);
            expect(component.selectedPrioritizers[0].type).toBe(
                'org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'
            );
            expect(component.availablePrioritizers).toHaveLength(2);
        });

        it('should filter selected prioritizers from available', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { component } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            const foundInAvailable = component.availablePrioritizers.find(
                (p) => p.type === 'org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'
            );
            expect(foundInAvailable).toBeUndefined();
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
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            component.writeValue(selectedTypes);
            expect(component.value).toEqual(selectedTypes);
        });

        it('should emit serialized prioritizers', async () => {
            const allPrioritizers = createAllPrioritizers();
            const { component } = await setup({ allPrioritizers });

            const onChangeSpy = jest.fn();
            component.registerOnChange(onChangeSpy);

            // Set up selected prioritizers
            component.writeValue(['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer']);

            // Manually call handleChanged to test serialization
            component['handleChanged']();

            expect(onChangeSpy).toHaveBeenCalledWith(['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer']);
        });
    });

    describe('removeSelected method logic', () => {
        it('should remove selected prioritizer and update arrays', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { component } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            const onChangeSpy = jest.fn();
            component.registerOnChange(onChangeSpy);

            component.removeSelected(component.selectedPrioritizers[0], 0);

            expect(component.selectedPrioritizers).toHaveLength(0);
            expect(component.availablePrioritizers).toHaveLength(3);
            expect(onChangeSpy).toHaveBeenCalledWith([]);
        });

        it('should call touch and change events', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { component } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            const onChangeSpy = jest.fn();
            const onTouchedSpy = jest.fn();
            component.registerOnChange(onChangeSpy);
            component.registerOnTouched(onTouchedSpy);

            component.removeSelected(component.selectedPrioritizers[0], 0);

            expect(component.isTouched).toBe(true);
            expect(onTouchedSpy).toHaveBeenCalled();
            expect(onChangeSpy).toHaveBeenCalled();
        });
    });

    describe('Helper methods', () => {
        it('should get prioritizer label', async () => {
            const { component } = await setup();
            const prioritizer = createDocumentedType('org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer');

            const label = component.getPrioritizerLabel(prioritizer);

            expect(mockNiFiCommon.getComponentTypeLabel).toHaveBeenCalledWith(prioritizer.type);
            expect(label).toBe('FirstInFirstOutPrioritizer');
        });

        it('should check if prioritizer has description', async () => {
            const { component } = await setup();
            const prioritizerWithDescription = createDocumentedType('test.Type', { description: 'Test description' });
            const prioritizerWithoutDescription = createDocumentedType('test.Type', { description: '' });

            const hasDescription1 = component.hasDescription(prioritizerWithDescription);
            const hasDescription2 = component.hasDescription(prioritizerWithoutDescription);

            expect(hasDescription1).toBe(true);
            expect(hasDescription2).toBe(false);
        });
    });

    describe('Template logic', () => {
        it('should show available prioritizers section when enabled', async () => {
            const { fixture } = await setup({ disabled: false });

            const availableSection = fixture.nativeElement.querySelector('[data-qa="available-prioritizers-section"]');
            expect(availableSection).toBeTruthy();
        });

        it('should hide available prioritizers section when disabled', async () => {
            const { fixture } = await setup({ disabled: true });

            const availableSection = fixture.nativeElement.querySelector('[data-qa="available-prioritizers-section"]');
            expect(availableSection).toBeFalsy();
        });

        it('should show selected prioritizers list when has selected items', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { fixture } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            const selectedList = fixture.nativeElement.querySelector('[data-qa="selected-prioritizers-list"]');
            expect(selectedList).toBeTruthy();
        });

        it('should show no selected prioritizers message when disabled and no items', async () => {
            const { fixture } = await setup({ disabled: true });

            const noSelectedMessage = fixture.nativeElement.querySelector('[data-qa="no-selected-prioritizers"]');
            expect(noSelectedMessage).toBeTruthy();
            expect(noSelectedMessage.textContent.trim()).toBe('No value set');
        });

        it('should display prioritizer names correctly', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { fixture } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            const prioritizerNames = fixture.nativeElement.querySelectorAll('[data-qa="prioritizer-name"]');
            expect(prioritizerNames.length).toBeGreaterThan(0);

            // Find the selected prioritizer name in the template
            const selectedPrioritizerName = Array.from(prioritizerNames).find(
                (element: any) => element.textContent.trim() === 'FirstInFirstOutPrioritizer'
            );
            expect(selectedPrioritizerName).toBeTruthy();
        });

        it('should show remove button for selected prioritizers when enabled', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { fixture } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes });

            const removeButton = fixture.nativeElement.querySelector('[data-qa="remove-prioritizer-button"]');
            expect(removeButton).toBeTruthy();
        });

        it('should hide remove button when disabled', async () => {
            const allPrioritizers = createAllPrioritizers();
            const selectedTypes = ['org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer'];
            const { fixture } = await setup({ allPrioritizers, selectedPrioritizers: selectedTypes, disabled: true });

            const removeButton = fixture.nativeElement.querySelector('[data-qa="remove-prioritizer-button"]');
            expect(removeButton).toBeFalsy();
        });
    });
});
