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
import { CounterTable } from './counter-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { CounterEntity } from '../../../state/counter-listing';
import { By } from '@angular/platform-browser';

describe('CounterTable', () => {
    // Mock data factories
    function createMockCounter(
        id: string,
        name: string,
        context: string,
        value: string,
        valueCount: number
    ): CounterEntity {
        return {
            id,
            name,
            context,
            value,
            valueCount
        };
    }

    function createMockCounters(): CounterEntity[] {
        return [
            createMockCounter('counter1', 'Test Counter 1', 'TestContext1', '5', 5),
            createMockCounter('counter2', 'Test Counter 2', 'TestContext2', '10', 10)
        ];
    }

    // Setup function for component configuration
    async function setup(
        options: {
            canModifyCounters?: boolean;
            counters?: CounterEntity[];
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [CounterTable, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(CounterTable);
        const component = fixture.componentInstance;

        // Apply options
        component.canModifyCounters = options.canModifyCounters ?? true;
        component.counters = options.counters ?? createMockCounters();

        fixture.detectChanges();

        return { component, fixture };
    }

    beforeEach(() => {
        // Clear any previous state if needed
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should include reset column when canModifyCounters is true', async () => {
            const { component } = await setup({ canModifyCounters: true });
            expect(component.displayedColumns).toContain('reset');
        });

        it('should not include reset column when canModifyCounters is false', async () => {
            const { component } = await setup({ canModifyCounters: false });
            expect(component.displayedColumns).not.toContain('reset');
        });
    });

    describe('Event emission', () => {
        it('should next resetCounter event when reset button is clicked', async () => {
            const { component } = await setup();
            const mockCounters = createMockCounters();
            jest.spyOn(component.resetCounter, 'next');

            component.resetClicked(mockCounters[0], new MouseEvent('click'));
            expect(component.resetCounter.next).toHaveBeenCalledWith(mockCounters[0]);
        });

        it('should next resetAllCounters event when reset all button is clicked', async () => {
            const { component } = await setup();
            jest.spyOn(component.resetAllCounters, 'next');

            component.resetAllClicked();
            expect(component.resetAllCounters.next).toHaveBeenCalled();
        });
    });

    describe('Reset all button visibility', () => {
        it('should display reset all button when user can modify counters and counters exist', async () => {
            const { fixture } = await setup({ canModifyCounters: true });

            const resetAllButton = fixture.debugElement.query(By.css('button[data-qa="reset-all-counters-button"]'));
            expect(resetAllButton).toBeTruthy();
            expect(resetAllButton.nativeElement.textContent.trim()).toContain('Reset All');
        });

        it('should not display reset all button when user cannot modify counters', async () => {
            const { fixture } = await setup({ canModifyCounters: false });

            const resetAllButton = fixture.debugElement.query(By.css('button[data-qa="reset-all-counters-button"]'));
            expect(resetAllButton).toBeFalsy();
        });

        it('should not display reset all button when no counters exist', async () => {
            const { fixture } = await setup({ canModifyCounters: true, counters: [] });

            const resetAllButton = fixture.debugElement.query(By.css('button[data-qa="reset-all-counters-button"]'));
            expect(resetAllButton).toBeTruthy();
            expect(resetAllButton.nativeElement.disabled).toBeTruthy();
        });
    });

    describe('Data formatting and filtering', () => {
        it('should format counter values correctly', async () => {
            const { component } = await setup();
            const mockCounters = createMockCounters();

            expect(component.formatContext(mockCounters[0])).toBe('TestContext1');
            expect(component.formatName(mockCounters[0])).toBe('Test Counter 1');
            expect(component.formatValue(mockCounters[0])).toBe('5');
        });

        it('should filter counters by name', async () => {
            const { component } = await setup();

            component.applyFilter('Counter 1', 'name');
            expect(component.dataSource.filteredData.length).toBe(1);
            expect(component.dataSource.filteredData[0].name).toBe('Test Counter 1');
        });

        it('should filter counters by context', async () => {
            const { component } = await setup();

            component.applyFilter('TestContext2', 'context');
            expect(component.dataSource.filteredData.length).toBe(1);
            expect(component.dataSource.filteredData[0].context).toBe('TestContext2');
        });
    });
});
