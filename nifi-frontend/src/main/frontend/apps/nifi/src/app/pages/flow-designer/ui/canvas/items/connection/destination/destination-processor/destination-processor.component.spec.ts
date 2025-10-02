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
import { DestinationProcessor } from './destination-processor.component';

describe('DestinationProcessor Component', () => {
    // Mock data factories
    function createMockProcessor(
        id: string = 'processor-id',
        canRead: boolean = true,
        name: string = 'Test Processor'
    ) {
        return {
            id,
            permissions: { canRead, canWrite: true },
            component: { name }
        };
    }

    // Setup function for component configuration
    async function setup(
        options: {
            processor?: any;
            groupName?: string;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [DestinationProcessor]
        }).compileComponents();

        const fixture = TestBed.createComponent(DestinationProcessor);
        const component = fixture.componentInstance;

        // Set input properties if provided
        if (options.processor) {
            component.processor = options.processor;
        }
        if (options.groupName !== undefined) {
            component.groupName = options.groupName;
        }

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
            expect(component.processorName).toBeUndefined();
            expect(component.groupName).toBeUndefined();
        });
    });

    describe('Processor setter logic', () => {
        it('should set processor name from component name when canRead is true', async () => {
            const processor = createMockProcessor('proc-1', true, 'My Processor');
            const { component } = await setup({ processor });

            expect(component.processorName).toBe('My Processor');
        });

        it('should set processor name from id when canRead is false', async () => {
            const processor = createMockProcessor('proc-1', false, 'My Processor');
            const { component } = await setup({ processor });

            expect(component.processorName).toBe('proc-1');
        });

        it('should handle null processor', async () => {
            const { component } = await setup({ processor: null });
            expect(component.processorName).toBeUndefined();
        });

        it('should handle processor with empty name when canRead is true', async () => {
            const processor = createMockProcessor('proc-1', true, '');
            const { component } = await setup({ processor });

            expect(component.processorName).toBe('');
        });
    });

    describe('GroupName property logic', () => {
        it('should set groupName from input', async () => {
            const { component } = await setup({ groupName: 'Test Group' });

            expect(component.groupName).toBe('Test Group');
        });

        it('should handle empty groupName', async () => {
            const { component } = await setup({ groupName: '' });

            expect(component.groupName).toBe('');
        });

        it('should handle undefined groupName', async () => {
            const { component } = await setup({ groupName: undefined });

            expect(component.groupName).toBeUndefined();
        });
    });

    describe('Template logic', () => {
        it('should display "To Processor" label', async () => {
            const { fixture } = await setup();

            const label = fixture.nativeElement.querySelector('[data-qa="to-processor-label"]');
            expect(label).toBeTruthy();
            expect(label.textContent.trim()).toBe('To Processor');
        });

        it('should display "Within Group" label', async () => {
            const { fixture } = await setup();

            const label = fixture.nativeElement.querySelector('[data-qa="within-group-label"]');
            expect(label).toBeTruthy();
            expect(label.textContent.trim()).toBe('Within Group');
        });

        it('should display processor name when available', async () => {
            const processor = createMockProcessor('proc-1', true, 'My Test Processor');
            const { fixture } = await setup({ processor });

            const processorDisplay = fixture.nativeElement.querySelector('[data-qa="processor-name-display"]');
            expect(processorDisplay).toBeTruthy();
            expect(processorDisplay.textContent.trim()).toBe('My Test Processor');
            expect(processorDisplay.getAttribute('title')).toBe('My Test Processor');
        });

        it('should display group name when available', async () => {
            const { fixture } = await setup({ groupName: 'My Test Group' });

            const groupDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupDisplay).toBeTruthy();
            expect(groupDisplay.textContent.trim()).toBe('My Test Group');
            expect(groupDisplay.getAttribute('title')).toBe('My Test Group');
        });

        it('should handle empty processor name display', async () => {
            const processor = createMockProcessor('proc-1', true, '');
            const { fixture } = await setup({ processor });

            const processorDisplay = fixture.nativeElement.querySelector('[data-qa="processor-name-display"]');
            expect(processorDisplay).toBeTruthy();
            expect(processorDisplay.textContent.trim()).toBe('');
            expect(processorDisplay.getAttribute('title')).toBe('');
        });

        it('should handle empty group name display', async () => {
            const { fixture } = await setup({ groupName: '' });

            const groupDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupDisplay).toBeTruthy();
            expect(groupDisplay.textContent.trim()).toBe('');
            expect(groupDisplay.getAttribute('title')).toBe('');
        });

        it('should apply overflow styles to both name display elements', async () => {
            const processor = createMockProcessor('proc-1', true, 'Test Processor');
            const { fixture } = await setup({ processor, groupName: 'Test Group' });

            const processorDisplay = fixture.nativeElement.querySelector('[data-qa="processor-name-display"]');
            const groupDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');

            expect(processorDisplay.classList).toContain('overflow-ellipsis');
            expect(processorDisplay.classList).toContain('overflow-hidden');
            expect(processorDisplay.classList).toContain('whitespace-nowrap');

            expect(groupDisplay.classList).toContain('overflow-ellipsis');
            expect(groupDisplay.classList).toContain('overflow-hidden');
            expect(groupDisplay.classList).toContain('whitespace-nowrap');
        });
    });
});
