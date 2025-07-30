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

import { counterListingReducer, initialState } from './counter-listing.reducer';
import * as CounterListingActions from './counter-listing.actions';
import { CounterEntity, CounterListingState } from './index';
import { HttpErrorResponse } from '@angular/common/http';

describe('Counter Listing Reducer', () => {
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

    function createResetCounters(): CounterEntity[] {
        return [
            createMockCounter('counter1', 'Test Counter 1', 'TestContext1', '0', 0),
            createMockCounter('counter2', 'Test Counter 2', 'TestContext2', '0', 0)
        ];
    }

    it('should return the initial state', () => {
        const result = counterListingReducer(undefined, {} as any);
        expect(result).toBe(initialState);
    });

    describe('loadCounters', () => {
        it('should set status to loading', () => {
            const action = CounterListingActions.loadCounters();
            const result = counterListingReducer(initialState, action);

            expect(result.status).toBe('loading');
        });
    });

    describe('loadCountersSuccess action', () => {
        it('should set counters and update state', () => {
            const mockCounters = createMockCounters();
            const loadedTimestamp = '2023-01-01 12:00:00 EST';
            const action = CounterListingActions.loadCountersSuccess({
                response: {
                    counters: mockCounters,
                    loadedTimestamp
                }
            });

            const result = counterListingReducer(initialState, action);

            expect(result.counters).toEqual(mockCounters);
            expect(result.loadedTimestamp).toBe(loadedTimestamp);
            expect(result.status).toBe('success');
        });
    });

    describe('counterListingApiError action', () => {
        it('should set saving to false on error', () => {
            const error = new HttpErrorResponse({ status: 500 });
            const state: CounterListingState = {
                ...initialState,
                saving: true
            };
            const action = CounterListingActions.counterListingApiError({ errorResponse: error });

            const result = counterListingReducer(state, action);

            expect(result.saving).toBe(false);
        });
    });

    describe('resetCounterSuccess action', () => {
        it('should update specific counter after reset', () => {
            const mockCounters = createMockCounters();
            const state: CounterListingState = {
                ...initialState,
                counters: mockCounters
            };

            const resetCounter = { ...mockCounters[0], value: '0', valueCount: 0 };
            const action = CounterListingActions.resetCounterSuccess({
                response: { counter: resetCounter }
            });

            const result = counterListingReducer(state, action);

            expect(result.counters[0]).toEqual(resetCounter);
            expect(result.counters[1]).toEqual(mockCounters[1]); // Should remain unchanged
        });

        it('should handle counter not found gracefully', () => {
            const mockCounters = createMockCounters();
            const state: CounterListingState = {
                ...initialState,
                counters: mockCounters
            };

            const nonExistentCounter = createMockCounter('nonexistent', 'Non-existent Counter', 'TestContext', '0', 0);
            const action = CounterListingActions.resetCounterSuccess({
                response: { counter: nonExistentCounter }
            });

            const result = counterListingReducer(state, action);

            // Should not modify existing counters
            expect(result.counters).toEqual(mockCounters);
        });
    });

    describe('resetAllCountersSuccess action', () => {
        it('should replace all counters with reset values', () => {
            const mockCounters = createMockCounters();
            const resetCounters = createResetCounters();
            const state: CounterListingState = {
                ...initialState,
                counters: mockCounters,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };

            const newTimestamp = '2023-01-01 12:00:00 EST';
            const action = CounterListingActions.resetAllCountersSuccess({
                response: {
                    counters: resetCounters,
                    loadedTimestamp: newTimestamp
                }
            });

            const result = counterListingReducer(state, action);

            expect(result.counters).toEqual(resetCounters);
            expect(result.loadedTimestamp).toBe(newTimestamp);
            expect(result.status).toBe('success');

            // Verify all counters are reset to 0
            result.counters.forEach((counter) => {
                expect(counter.value).toBe('0');
                expect(counter.valueCount).toBe(0);
            });
        });

        it('should handle empty counters response', () => {
            const mockCounters = createMockCounters();
            const state: CounterListingState = {
                ...initialState,
                counters: mockCounters
            };

            const newTimestamp = '2023-01-01 12:00:00 EST';
            const action = CounterListingActions.resetAllCountersSuccess({
                response: {
                    counters: [],
                    loadedTimestamp: newTimestamp
                }
            });

            const result = counterListingReducer(state, action);

            expect(result.counters).toEqual([]);
            expect(result.loadedTimestamp).toBe(newTimestamp);
            expect(result.status).toBe('success');
        });
    });

    describe('resetCounterState action', () => {
        it('should reset to initial state', () => {
            const mockCounters = createMockCounters();
            const state: CounterListingState = {
                counters: mockCounters,
                saving: true,
                loadedTimestamp: '2023-01-01 12:00:00 EST',
                status: 'success'
            };

            const action = CounterListingActions.resetCounterState();
            const result = counterListingReducer(state, action);

            expect(result).toEqual(initialState);
        });
    });
});
