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
import { SummaryListingEffects } from './summary-listing.effects';
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from './summary-listing.reducer';
import { of, ReplaySubject, take, throwError } from 'rxjs';
import { Action } from '@ngrx/store';
import * as SummaryListingActions from './summary-listing.actions';
import { ProcessGroupStatusService } from '../../service/process-group-status.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { summaryFeatureKey } from '../index';
import { summaryListingFeatureKey } from './index';

describe('SummaryListingEffects', () => {
    interface SetupOptions {
        summaryListingState?: any;
    }

    const mockSummaryResponse = {
        status: {
            canRead: true,
            processGroupStatus: {
                id: 'root',
                name: 'NiFi Flow',
                statsLastRefreshed: '2023-01-01 12:00:00 EST',
                aggregateSnapshot: {
                    id: 'root',
                    name: 'NiFi Flow',
                    flowFilesIn: 0,
                    bytesIn: 0,
                    input: '0 / 0 bytes',
                    flowFilesOut: 0,
                    bytesOut: 0,
                    output: '0 / 0 bytes',
                    flowFilesTransferred: 0,
                    bytesTransferred: 0,
                    transferred: '0 / 0 bytes',
                    bytesReceived: 0,
                    flowFilesReceived: 0,
                    received: '0 / 0 bytes',
                    bytesSent: 0,
                    flowFilesSent: 0,
                    sent: '0 / 0 bytes',
                    bytesRead: 0,
                    read: '0 bytes',
                    bytesWritten: 0,
                    written: '0 bytes',
                    activeThreadCount: 0,
                    terminatedThreadCount: 0,
                    processingNanos: 0,
                    statelessActiveThreadCount: 0,
                    processorStatusSnapshots: [],
                    connectionStatusSnapshots: [],
                    processGroupStatusSnapshots: [],
                    inputPortStatusSnapshots: [],
                    outputPortStatusSnapshots: [],
                    remoteProcessGroupStatusSnapshots: []
                }
            }
        }
    };

    let action$: ReplaySubject<Action>;

    async function setup({ summaryListingState = initialState }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                SummaryListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [summaryFeatureKey]: {
                            [summaryListingFeatureKey]: summaryListingState || initialState
                        }
                    }
                }),
                {
                    provide: ProcessGroupStatusService,
                    useValue: {
                        getProcessGroupsStatus: jest.fn()
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
        const summaryService = TestBed.inject(ProcessGroupStatusService) as jest.Mocked<ProcessGroupStatusService>;
        const errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        const effects = TestBed.inject(SummaryListingEffects);

        return { store, summaryService, errorHelper, effects };
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

    describe('loadSummaryListing', () => {
        it('should load summary listing successfully', async () => {
            const { effects, summaryService } = await setup();

            action$.next(SummaryListingActions.loadSummaryListing({ recursive: false }));
            jest.spyOn(summaryService, 'getProcessGroupsStatus').mockReturnValueOnce(
                of(mockSummaryResponse.status) as never
            );

            const result = await new Promise((resolve) => effects.loadSummaryListing$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                SummaryListingActions.loadSummaryListingSuccess({
                    response: {
                        status: mockSummaryResponse.status
                    }
                })
            );
        });

        it('should fail to load summary listing on initial load with hasExistingData=false', async () => {
            const { effects, summaryService } = await setup();

            action$.next(SummaryListingActions.loadSummaryListing({ recursive: false }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(summaryService, 'getProcessGroupsStatus').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadSummaryListing$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                SummaryListingActions.loadSummaryListingError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('should fail to load summary listing on refresh with hasExistingData=true', async () => {
            const stateWithData = {
                ...initialState,
                loadedTimestamp: '2023-01-01 11:00:00 EST'
            };
            const { effects, summaryService } = await setup({ summaryListingState: stateWithData });

            action$.next(SummaryListingActions.loadSummaryListing({ recursive: false }));
            const error = new HttpErrorResponse({ status: 500 });
            jest.spyOn(summaryService, 'getProcessGroupsStatus').mockImplementationOnce(() => throwError(() => error));

            const result = await new Promise((resolve) => effects.loadSummaryListing$.pipe(take(1)).subscribe(resolve));

            expect(result).toEqual(
                SummaryListingActions.loadSummaryListingError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });

        it('should handle loadSummaryListingError with pending status (no existing data)', async () => {
            const { effects, errorHelper } = await setup();

            action$.next(
                SummaryListingActions.loadSummaryListingError({
                    errorResponse: new HttpErrorResponse({ status: 500 }),
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );

            const errorAction = SummaryListingActions.loadSummaryListingSuccess({
                response: {
                    status: mockSummaryResponse.status
                }
            });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadSummaryListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, expect.any(HttpErrorResponse));
            expect(result).toEqual(errorAction);
        });

        it('should handle loadSummaryListingError with success status (existing data)', async () => {
            const { effects, errorHelper } = await setup();

            action$.next(
                SummaryListingActions.loadSummaryListingError({
                    errorResponse: new HttpErrorResponse({ status: 503 }),
                    loadedTimestamp: '2023-01-01 11:00:00 EST',
                    status: 'success'
                })
            );

            const errorAction = SummaryListingActions.loadSummaryListingSuccess({
                response: {
                    status: mockSummaryResponse.status
                }
            });
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadSummaryListingError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, expect.any(HttpErrorResponse));
            expect(result).toEqual(errorAction);
        });
    });
});
