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
import { FlowConfigurationHistoryListingEffects } from './flow-configuration-history-listing.effects';
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialHistoryState } from './flow-configuration-history-listing.reducer';
import { of, ReplaySubject, take, throwError } from 'rxjs';
import { Action } from '@ngrx/store';
import * as HistoryActions from './flow-configuration-history-listing.actions';
import { FlowConfigurationHistoryService } from '../../service/flow-configuration-history.service';
import { MatDialog } from '@angular/material/dialog';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { flowConfigurationHistoryFeatureKey } from '../index';
import { flowConfigurationHistoryListingFeatureKey } from './index';

describe('FlowConfigurationHistoryListingEffects', () => {
    interface SetupOptions {
        historyListingState?: any;
    }

    const mockHistoryResponse = {
        history: {
            actions: [
                {
                    id: 1,
                    timestamp: '2023-01-01 12:00:00 EST',
                    userIdentity: 'admin',
                    operation: 'Create',
                    componentDetails: {
                        name: 'Test Component',
                        type: 'Processor'
                    },
                    sourceId: 'source-1',
                    canRead: true
                }
            ],
            total: 1,
            lastRefreshed: '2023-01-01 12:00:00 EST'
        }
    };

    let action$: ReplaySubject<Action>;

    async function setup({ historyListingState = initialHistoryState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                FlowConfigurationHistoryListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [flowConfigurationHistoryFeatureKey]: {
                            [flowConfigurationHistoryListingFeatureKey]: historyListingState || initialHistoryState
                        }
                    }
                }),
                {
                    provide: FlowConfigurationHistoryService,
                    useValue: {
                        getHistory: jest.fn(),
                        purgeHistory: jest.fn()
                    }
                },
                {
                    provide: MatDialog,
                    useValue: {
                        open: jest.fn()
                    }
                },
                {
                    provide: Router,
                    useValue: {
                        navigate: jest.fn()
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
        const historyService = TestBed.inject(
            FlowConfigurationHistoryService
        ) as jest.Mocked<FlowConfigurationHistoryService>;
        const errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        const effects = TestBed.inject(FlowConfigurationHistoryListingEffects);

        return { store, historyService, errorHelper, effects };
    }

    beforeEach(() => {
        action$ = new ReplaySubject<Action>(1);
    });

    afterEach(() => {
        action$.complete();
    });

    it('should be created', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('loadHistory', () => {
        it('should load history successfully', async () => {
            const { effects, historyService } = await setup();

            const request = { count: 50, offset: 0 };
            action$.next(HistoryActions.loadHistory({ request }));
            jest.spyOn(historyService, 'getHistory').mockReturnValueOnce(of(mockHistoryResponse) as never);

            const result = await new Promise((resolve) => effects.loadHistory$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                HistoryActions.loadHistorySuccess({
                    response: mockHistoryResponse
                })
            );
        });

        it('should fail to load history on initial load with hasExistingData=false', async () => {
            const { effects, historyService } = await setup();

            const request = { count: 50, offset: 0 };
            action$.next(HistoryActions.loadHistory({ request }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(historyService, 'getHistory').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadHistory$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                HistoryActions.loadHistoryError({
                    errorResponse: error,
                    loadedTimestamp: initialHistoryState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load history on refresh with hasExistingData=true', async () => {
            const stateWithData = {
                ...initialHistoryState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { effects, historyService } = await setup({ historyListingState: stateWithData });

            const request = { count: 50, offset: 0 };
            action$.next(HistoryActions.loadHistory({ request }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(historyService, 'getHistory').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadHistory$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                HistoryActions.loadHistoryError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('Load History Error', () => {
        it('should handle history error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = HistoryActions.flowConfigurationHistorySnackbarError({ errorResponse: error });
            action$.next(
                HistoryActions.loadHistoryError({
                    errorResponse: error,
                    loadedTimestamp: initialHistoryState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) => effects.loadHistoryError$.pipe(take(1)).subscribe(resolve));

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle history error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = HistoryActions.flowConfigurationHistorySnackbarError({ errorResponse: error });
            action$.next(
                HistoryActions.loadHistoryError({
                    errorResponse: error,
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) => effects.loadHistoryError$.pipe(take(1)).subscribe(resolve));

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });
});
