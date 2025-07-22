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

import { CounterListing } from './counter-listing.component';
import { TestBed } from '@angular/core/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/counter-listing/counter-listing.reducer';
import { CounterEntity } from '../../state/counter-listing';
import {
    loadCounters,
    promptCounterReset,
    promptResetAllCounters
} from '../../state/counter-listing/counter-listing.actions';

describe('CounterListing', () => {
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
    async function setup() {
        await TestBed.configureTestingModule({
            imports: [CounterListing],
            providers: [provideMockStore({ initialState })]
        }).compileComponents();

        const fixture = TestBed.createComponent(CounterListing);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);

        fixture.detectChanges();

        return { component, fixture, store };
    }

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should dispatch loadCounters action on ngOnInit', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.ngOnInit();
            expect(store.dispatch).toHaveBeenCalledWith(loadCounters());
        });
    });

    describe('Action dispatching', () => {
        it('should dispatch loadCounters action when refreshCounterListing is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            component.refreshCounterListing();
            expect(store.dispatch).toHaveBeenCalledWith(loadCounters());
        });

        it('should dispatch promptCounterReset action when resetCounter is called', async () => {
            const { component, store } = await setup();
            const mockCounters = createMockCounters();
            jest.spyOn(store, 'dispatch');
            const testCounter = mockCounters[0];

            component.resetCounter(testCounter);

            expect(store.dispatch).toHaveBeenCalledWith(
                promptCounterReset({
                    request: {
                        counter: testCounter
                    }
                })
            );
        });

        it('should dispatch promptResetAllCounters action when resetAllCounters is called', async () => {
            const { component, store } = await setup();
            jest.spyOn(store, 'dispatch');

            const counterCount = 6;
            component.resetAllCounters(counterCount);

            expect(store.dispatch).toHaveBeenCalledWith(
                promptResetAllCounters({
                    request: {
                        counterCount
                    }
                })
            );
        });
    });

    describe('Loading state logic', () => {
        it('should return true for isInitialLoading when timestamp matches initial state', async () => {
            const { component } = await setup();
            const state = { ...initialState };

            expect(component.isInitialLoading(state)).toBe(true);
        });

        it('should return false for isInitialLoading when timestamp differs from initial state', async () => {
            const { component } = await setup();
            const state = {
                ...initialState,
                loadedTimestamp: '2023-01-01 12:00:00 EST'
            };

            expect(component.isInitialLoading(state)).toBe(false);
        });
    });
});
