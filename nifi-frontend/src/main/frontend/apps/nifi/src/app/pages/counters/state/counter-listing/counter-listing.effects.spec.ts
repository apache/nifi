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
import { CounterListingEffects } from './counter-listing.effects';
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from './counter-listing.reducer';
import { of, ReplaySubject, take, throwError } from 'rxjs';
import { Action } from '@ngrx/store';
import * as CounterListingActions from './counter-listing.actions';
import { CountersService } from '../../service/counters.service';
import { MatDialog } from '@angular/material/dialog';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { CounterEntity } from './index';
import { countersFeatureKey } from '../index';

describe('CounterListingEffects', () => {
    let action$: ReplaySubject<Action>;

    interface SetupOptions {
        counterListingState?: any;
    }

    const mockCounters: CounterEntity[] = [
        {
            id: 'counter1',
            name: 'Test Counter 1',
            context: 'TestContext1',
            value: '0',
            valueCount: 0
        },
        {
            id: 'counter2',
            name: 'Test Counter 2',
            context: 'TestContext2',
            value: '0',
            valueCount: 0
        }
    ];

    const mockCountersResponse = {
        counters: {
            aggregateSnapshot: {
                counters: mockCounters,
                generated: '2023-01-01 12:00:00 EST'
            }
        }
    };

    async function setup({ counterListingState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                CounterListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [countersFeatureKey]: {
                            [countersFeatureKey]: counterListingState
                        }
                    }
                }),
                {
                    provide: CountersService,
                    useValue: {
                        getCounters: jest.fn(),
                        resetCounter: jest.fn(),
                        resetAllCounters: jest.fn()
                    }
                },
                {
                    provide: MatDialog,
                    useValue: {
                        open: jest.fn()
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        handleLoadingError: jest.fn(),
                        getErrorString: jest.fn()
                    }
                }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const dispatchSpy = jest.spyOn(store, 'dispatch');

        const effects = TestBed.inject(CounterListingEffects);
        action$ = new ReplaySubject<Action>();

        const countersService = TestBed.inject(CountersService);
        const dialog = TestBed.inject(MatDialog);
        const errorHelper = TestBed.inject(ErrorHelper);

        return { effects, action$, store, dispatchSpy, countersService, dialog, errorHelper };
    }

    afterEach(() => {
        if (action$) {
            action$.complete();
        }
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('Load Counters', () => {
        it('should load counters successfully', async () => {
            const { action$, effects, countersService } = await setup();

            action$.next(CounterListingActions.loadCounters());

            jest.spyOn(countersService, 'getCounters').mockReturnValueOnce(of(mockCountersResponse) as never);

            const result = await new Promise((resolve) => effects.loadCounters$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                CounterListingActions.loadCountersSuccess({
                    response: {
                        counters: mockCounters,
                        loadedTimestamp: '2023-01-01 12:00:00 EST'
                    }
                })
            );
        });

        it('should fail to load counters on initial load with hasExistingData=false', async () => {
            const { action$, effects, countersService } = await setup();

            action$.next(CounterListingActions.loadCounters());

            const error = new HttpErrorResponse({ status: 500 });

            jest.spyOn(countersService, 'getCounters').mockImplementationOnce(() => {
                return throwError(() => error);
            });

            const result = await new Promise((resolve) => effects.loadCounters$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                CounterListingActions.loadCountersError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load counters on refresh with hasExistingData=true', async () => {
            const stateWithData = {
                ...initialState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { action$, effects, countersService } = await setup({
                counterListingState: stateWithData
            });

            action$.next(CounterListingActions.loadCounters());

            const error = new HttpErrorResponse({ status: 500 });

            jest.spyOn(countersService, 'getCounters').mockImplementationOnce(() => {
                return throwError(() => error);
            });

            const result = await new Promise((resolve) => effects.loadCounters$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                CounterListingActions.loadCountersError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Counter Listing Error', () => {
        it('should handle counter listing error for initial load', async () => {
            const { action$, effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = CounterListingActions.counterListingApiError({ errorResponse: error });
            action$.next(
                CounterListingActions.loadCountersError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.counterListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle counter listing error for refresh', async () => {
            const { action$, effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = CounterListingActions.counterListingApiError({ errorResponse: error });
            action$.next(
                CounterListingActions.loadCountersError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.counterListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });

    describe('Prompt Reset All Counters', () => {
        it('should open confirmation dialog and dispatch resetAllCounters on yes', async () => {
            const { action$, effects, dialog, dispatchSpy } = await setup();

            const dialogRef = {
                componentInstance: {
                    yes: of(true)
                }
            };

            action$.next(
                CounterListingActions.promptResetAllCounters({
                    request: {
                        counterCount: 6
                    }
                })
            );

            jest.spyOn(dialog, 'open').mockReturnValueOnce(dialogRef as any);

            await new Promise((resolve) => effects.promptResetAllCounters$.pipe(take(1)).subscribe(resolve));

            expect(dialog.open).toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalledWith(CounterListingActions.resetAllCounters());
        });
    });

    describe('Reset All Counters', () => {
        it('should reset all counters successfully', async () => {
            const { action$, effects, countersService } = await setup();

            action$.next(CounterListingActions.resetAllCounters());

            jest.spyOn(countersService, 'resetAllCounters').mockReturnValueOnce(of(mockCountersResponse) as never);

            const result = await new Promise((resolve) => effects.resetAllCounters$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                CounterListingActions.resetAllCountersSuccess({
                    response: {
                        counters: mockCounters,
                        loadedTimestamp: '2023-01-01 12:00:00 EST'
                    }
                })
            );
        });

        it('should fail to reset all counters', async () => {
            const { action$, effects, countersService } = await setup();

            action$.next(CounterListingActions.resetAllCounters());

            const error = new HttpErrorResponse({ status: 500 });

            jest.spyOn(countersService, 'resetAllCounters').mockImplementationOnce(() => {
                return throwError(() => error);
            });

            const result = await new Promise((resolve) => effects.resetAllCounters$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(CounterListingActions.counterListingApiError({ errorResponse: error }));
        });
    });

    describe('Reset Counter', () => {
        it('should reset counter successfully', async () => {
            const { action$, effects, countersService } = await setup();

            const request = { counter: mockCounters[0] };
            const response = { counter: { ...mockCounters[0], value: '0', valueCount: 0 } };

            action$.next(CounterListingActions.resetCounter({ request }));

            jest.spyOn(countersService, 'resetCounter').mockReturnValueOnce(of(response) as never);

            const result = await new Promise((resolve) => effects.resetCounter$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(CounterListingActions.resetCounterSuccess({ response }));
        });

        it('should fail to reset counter', async () => {
            const { action$, effects, countersService } = await setup();

            const request = { counter: mockCounters[0] };
            const error = new HttpErrorResponse({ status: 500 });

            action$.next(CounterListingActions.resetCounter({ request }));

            jest.spyOn(countersService, 'resetCounter').mockImplementationOnce(() => {
                return throwError(() => error);
            });

            const result = await new Promise((resolve) => effects.resetCounter$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(CounterListingActions.counterListingApiError({ errorResponse: error }));
        });
    });
});
