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
import { DestinationOutputPort } from './destination-output-port.component';

describe('DestinationOutputPort Component', () => {
    // Setup function for component configuration
    async function setup(
        options: {
            outputPortName?: string;
            groupName?: string;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [DestinationOutputPort]
        }).compileComponents();

        const fixture = TestBed.createComponent(DestinationOutputPort);
        const component = fixture.componentInstance;

        // Set input properties
        if (options.outputPortName !== undefined) {
            component.outputPortName = options.outputPortName;
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

        it('should initialize with provided values', async () => {
            const { component } = await setup({
                outputPortName: 'Test Output Port',
                groupName: 'Test Group'
            });

            expect(component.name).toBe('Test Output Port');
            expect(component.groupName).toBe('Test Group');
        });

        it('should handle empty values', async () => {
            const { component } = await setup({
                outputPortName: '',
                groupName: ''
            });

            expect(component.name).toBe('');
            expect(component.groupName).toBe('');
        });
    });

    describe('Input property logic', () => {
        it('should set name property when outputPortName is set', async () => {
            const { component } = await setup();

            component.outputPortName = 'New Output Port Name';
            expect(component.name).toBe('New Output Port Name');
        });

        it('should update name when outputPortName changes', async () => {
            const { component } = await setup({ outputPortName: 'Initial Name' });

            expect(component.name).toBe('Initial Name');

            component.outputPortName = 'Updated Name';
            expect(component.name).toBe('Updated Name');
        });

        it('should accept and store groupName input', async () => {
            const { component } = await setup();

            component.groupName = 'New Group Name';
            expect(component.groupName).toBe('New Group Name');
        });

        it('should update groupName when input changes', async () => {
            const { component } = await setup({ groupName: 'Initial Group' });

            expect(component.groupName).toBe('Initial Group');

            component.groupName = 'Updated Group';
            expect(component.groupName).toBe('Updated Group');
        });

        it('should handle special characters in names', async () => {
            const specialOutputPortName = 'Output-Port_With!Special@Characters#123';
            const specialGroupName = 'Group-Name_With!Special@Characters#123';
            const { component } = await setup({
                outputPortName: specialOutputPortName,
                groupName: specialGroupName
            });

            expect(component.name).toBe(specialOutputPortName);
            expect(component.groupName).toBe(specialGroupName);
        });
    });

    describe('Template logic', () => {
        it('should display "To Output" label', async () => {
            const { fixture } = await setup();

            const toOutputLabel = fixture.nativeElement.querySelector('[data-qa="to-output-label"]');
            expect(toOutputLabel).toBeTruthy();
            expect(toOutputLabel.textContent.trim()).toBe('To Output');
        });

        it('should display output port name in template', async () => {
            const testOutputPortName = 'My Test Output Port';
            const { fixture } = await setup({ outputPortName: testOutputPortName });

            const outputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="output-port-name-display"]');
            expect(outputPortNameDisplay).toBeTruthy();
            expect(outputPortNameDisplay.textContent.trim()).toBe(testOutputPortName);
        });

        it('should display "Within Group" label', async () => {
            const { fixture } = await setup();

            const withinGroupLabel = fixture.nativeElement.querySelector('[data-qa="within-group-label"]');
            expect(withinGroupLabel).toBeTruthy();
            expect(withinGroupLabel.textContent.trim()).toBe('Within Group');
        });

        it('should display groupName in template', async () => {
            const testGroupName = 'My Test Group';
            const { fixture } = await setup({ groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe(testGroupName);
        });

        it('should update output port name display when input changes', async () => {
            const { fixture, component } = await setup({ outputPortName: 'Initial Output Port' });

            // Verify initial display
            let outputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="output-port-name-display"]');
            expect(outputPortNameDisplay.textContent.trim()).toBe('Initial Output Port');

            // Change the input
            component.outputPortName = 'Updated Output Port';
            fixture.detectChanges();

            // Verify updated display
            outputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="output-port-name-display"]');
            expect(outputPortNameDisplay.textContent.trim()).toBe('Updated Output Port');
        });

        it('should update groupName display when input changes', async () => {
            const { fixture, component } = await setup({ groupName: 'Initial Group' });

            // Verify initial display
            let groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.textContent.trim()).toBe('Initial Group');

            // Change the input
            component.groupName = 'Updated Group';
            fixture.detectChanges();

            // Verify updated display
            groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.textContent.trim()).toBe('Updated Group');
        });

        it('should set title attribute for output port name element', async () => {
            const testOutputPortName = 'Output Port with Title';
            const { fixture } = await setup({ outputPortName: testOutputPortName });

            const outputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="output-port-name-display"]');
            expect(outputPortNameDisplay.getAttribute('title')).toBe(testOutputPortName);
        });

        it('should set title attribute for groupName element', async () => {
            const testGroupName = 'Group with Title';
            const { fixture } = await setup({ groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.getAttribute('title')).toBe(testGroupName);
        });
    });
});
