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

import { SourceFunnel } from './source-funnel.component';

describe('SourceFunnel', () => {
    // Setup function for component configuration
    async function setup(
        options: {
            groupName?: string;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [SourceFunnel]
        }).compileComponents();

        const fixture = TestBed.createComponent(SourceFunnel);
        const component = fixture.componentInstance;

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
            const { component } = await setup({ groupName: 'Test Group' });
            expect(component).toBeTruthy();
        });

        it('should initialize with provided groupName', async () => {
            const { component } = await setup({ groupName: 'Test Group' });
            expect(component.groupName).toBe('Test Group');
        });

        it('should initialize without groupName', async () => {
            const { component } = await setup();
            expect(component.groupName).toBeUndefined();
        });
    });

    describe('Input property logic', () => {
        it('should accept groupName input', async () => {
            const { component } = await setup({ groupName: 'Initial Group' });

            component.groupName = 'Updated Group';

            expect(component.groupName).toBe('Updated Group');
        });

        it('should handle empty groupName', async () => {
            const { component } = await setup({ groupName: '' });
            expect(component.groupName).toBe('');
        });

        it('should update groupName after initialization', async () => {
            const { component, fixture } = await setup({ groupName: 'Initial Group' });

            component.groupName = 'Updated Group';
            fixture.detectChanges();

            expect(component.groupName).toBe('Updated Group');
        });
    });

    describe('Template logic', () => {
        it('should display funnel label', async () => {
            const { fixture } = await setup({ groupName: 'Test Group' });

            const funnelLabel = fixture.nativeElement.querySelector('[data-qa="funnel-label"]');
            expect(funnelLabel).toBeTruthy();
            expect(funnelLabel.textContent.trim()).toBe('funnel');
        });

        it('should display groupName when provided', async () => {
            const testGroupName = 'Test Group Name';
            const { fixture } = await setup({ groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe(testGroupName);
        });

        it('should set title attribute for groupName', async () => {
            const testGroupName = 'Test Group Name';
            const { fixture } = await setup({ groupName: testGroupName });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.getAttribute('title')).toBe(testGroupName);
        });

        it('should display empty groupName', async () => {
            const { fixture } = await setup({ groupName: '' });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('');
            expect(groupNameDisplay.getAttribute('title')).toBe('');
        });

        it('should update display when groupName changes', async () => {
            const { component, fixture } = await setup({ groupName: 'Initial Group' });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.textContent.trim()).toBe('Initial Group');

            component.groupName = 'Updated Group';
            fixture.detectChanges();

            expect(groupNameDisplay.textContent.trim()).toBe('Updated Group');
            expect(groupNameDisplay.getAttribute('title')).toBe('Updated Group');
        });
    });
});
