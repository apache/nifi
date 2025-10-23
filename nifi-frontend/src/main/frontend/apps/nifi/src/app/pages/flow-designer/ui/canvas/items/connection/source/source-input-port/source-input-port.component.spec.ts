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

import { SourceInputPort } from './source-input-port.component';

describe('SourceInputPort', () => {
    // Setup function for component configuration
    async function setup(
        options: {
            inputPortName?: string;
            groupName?: string;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [SourceInputPort]
        }).compileComponents();

        const fixture = TestBed.createComponent(SourceInputPort);
        const component = fixture.componentInstance;

        // Set inputPortName if provided
        if (options.inputPortName !== undefined) {
            component.inputPortName = options.inputPortName;
        }

        // Set groupName if provided
        if (options.groupName !== undefined) {
            component.groupName = options.groupName;
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
            const { component } = await setup({ inputPortName: 'Test Input', groupName: 'Test Group' });
            expect(component).toBeTruthy();
        });

        it('should initialize with provided inputPortName', async () => {
            const { component } = await setup({ inputPortName: 'Test Input Port', groupName: 'Test Group' });
            expect(component.name).toBe('Test Input Port');
        });

        it('should initialize with provided groupName', async () => {
            const { component } = await setup({ inputPortName: 'Test Input', groupName: 'Test Group' });
            expect(component.groupName).toBe('Test Group');
        });

        it('should initialize without inputPortName', async () => {
            const { component } = await setup({ groupName: 'Test Group' });
            expect(component.name).toBeUndefined();
        });

        it('should initialize without groupName', async () => {
            const { component } = await setup({ inputPortName: 'Test Input' });
            expect(component.groupName).toBeUndefined();
        });
    });

    describe('InputPortName setter logic', () => {
        it('should set name property when inputPortName is set', async () => {
            const { component } = await setup({ groupName: 'Test Group' });

            component.inputPortName = 'Custom Input Port';

            expect(component.name).toBe('Custom Input Port');
        });

        it('should handle empty inputPortName', async () => {
            const { component } = await setup({ inputPortName: '', groupName: 'Test Group' });
            expect(component.name).toBe('');
        });

        it('should update name when inputPortName changes', async () => {
            const { component } = await setup({ inputPortName: 'Initial Name', groupName: 'Test Group' });

            expect(component.name).toBe('Initial Name');

            component.inputPortName = 'Updated Name';

            expect(component.name).toBe('Updated Name');
        });
    });

    describe('Input property logic', () => {
        it('should accept groupName input', async () => {
            const { component } = await setup({ inputPortName: 'Test Input', groupName: 'Initial Group' });

            component.groupName = 'Updated Group';

            expect(component.groupName).toBe('Updated Group');
        });

        it('should handle empty groupName', async () => {
            const { component } = await setup({ inputPortName: 'Test Input', groupName: '' });
            expect(component.groupName).toBe('');
        });
    });

    describe('Template logic', () => {
        it('should display input port name when provided', async () => {
            const testInputPortName = 'Test Input Port Name';
            const { fixture } = await setup({ inputPortName: testInputPortName, groupName: 'Test Group' });

            const inputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="input-port-name-display"]');
            expect(inputPortNameDisplay).toBeTruthy();
            expect(inputPortNameDisplay.textContent.trim()).toBe(testInputPortName);
        });

        it('should set title attribute for input port name', async () => {
            const testInputPortName = 'Test Input Port Name';
            const { fixture } = await setup({ inputPortName: testInputPortName, groupName: 'Test Group' });

            const inputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="input-port-name-display"]');
            expect(inputPortNameDisplay.getAttribute('title')).toBe(testInputPortName);
        });

        it('should display groupName when provided', async () => {
            const testGroupName = 'Test Group Name';
            const { fixture } = await setup({ inputPortName: 'Test Input', groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe(testGroupName);
        });

        it('should set title attribute for groupName', async () => {
            const testGroupName = 'Test Group Name';
            const { fixture } = await setup({ inputPortName: 'Test Input', groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.getAttribute('title')).toBe(testGroupName);
        });

        it('should display empty input port name', async () => {
            const { fixture } = await setup({ inputPortName: '', groupName: 'Test Group' });

            const inputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="input-port-name-display"]');
            expect(inputPortNameDisplay).toBeTruthy();
            expect(inputPortNameDisplay.textContent.trim()).toBe('');
            expect(inputPortNameDisplay.getAttribute('title')).toBe('');
        });

        it('should display empty groupName', async () => {
            const { fixture } = await setup({ inputPortName: 'Test Input', groupName: '' });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('');
            expect(groupNameDisplay.getAttribute('title')).toBe('');
        });

        it('should update display when inputPortName changes', async () => {
            const { component, fixture } = await setup({ inputPortName: 'Initial Input', groupName: 'Test Group' });

            const inputPortNameDisplay = fixture.nativeElement.querySelector('[data-qa="input-port-name-display"]');
            expect(inputPortNameDisplay.textContent.trim()).toBe('Initial Input');

            component.inputPortName = 'Updated Input';
            fixture.detectChanges();

            expect(inputPortNameDisplay.textContent.trim()).toBe('Updated Input');
            expect(inputPortNameDisplay.getAttribute('title')).toBe('Updated Input');
        });

        it('should update display when groupName changes', async () => {
            const { component, fixture } = await setup({ inputPortName: 'Test Input', groupName: 'Initial Group' });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.textContent.trim()).toBe('Initial Group');

            component.groupName = 'Updated Group';
            fixture.detectChanges();

            expect(groupNameDisplay.textContent.trim()).toBe('Updated Group');
            expect(groupNameDisplay.getAttribute('title')).toBe('Updated Group');
        });
    });
});
