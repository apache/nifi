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
import { DestinationFunnel } from './destination-funnel.component';

describe('DestinationFunnel Component', () => {
    // Setup function for component configuration
    async function setup(
        options: {
            groupName?: string;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [DestinationFunnel]
        }).compileComponents();

        const fixture = TestBed.createComponent(DestinationFunnel);
        const component = fixture.componentInstance;

        // Set input properties
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

        it('should initialize with provided groupName', async () => {
            const { component } = await setup({ groupName: 'Test Group' });
            expect(component.groupName).toBe('Test Group');
        });

        it('should handle empty groupName', async () => {
            const { component } = await setup({ groupName: '' });
            expect(component.groupName).toBe('');
        });
    });

    describe('Input property logic', () => {
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

        it('should handle special characters in groupName', async () => {
            const specialName = 'Group-Name_With!Special@Characters#123';
            const { component } = await setup({ groupName: specialName });

            expect(component.groupName).toBe(specialName);
        });
    });

    describe('Template logic', () => {
        it('should display "To Funnel" label', async () => {
            const { fixture } = await setup();

            const toFunnelLabel = fixture.nativeElement.querySelector('[data-qa="to-funnel-label"]');
            expect(toFunnelLabel).toBeTruthy();
            expect(toFunnelLabel.textContent.trim()).toBe('To Funnel');
        });

        it('should display static "funnel" text', async () => {
            const { fixture } = await setup();

            const funnelText = fixture.nativeElement.querySelector('[data-qa="funnel-text"]');
            expect(funnelText).toBeTruthy();
            expect(funnelText.textContent.trim()).toBe('funnel');
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

        it('should set title attribute for groupName element', async () => {
            const testGroupName = 'Group with Title';
            const { fixture } = await setup({ groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.getAttribute('title')).toBe(testGroupName);
        });
    });
});
