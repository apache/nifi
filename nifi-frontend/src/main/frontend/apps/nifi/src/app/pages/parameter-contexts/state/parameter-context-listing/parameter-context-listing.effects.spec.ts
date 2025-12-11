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
import { provideMockActions } from '@ngrx/effects/testing';
import { Action } from '@ngrx/store';
import { ReplaySubject, of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';
import { provideMockStore } from '@ngrx/store/testing';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';

import { ParameterContextListingEffects } from './parameter-context-listing.effects';
import * as ParameterContextListingActions from './parameter-context-listing.actions';
import { ParameterContextService } from '../../service/parameter-contexts.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { Storage } from '@nifi/shared';
import { initialState } from './parameter-context-listing.reducer';
import { ParameterContextUpdateRequest, ParameterContextUpdateRequestEntity } from '../../../../state/shared';
import { HttpErrorResponse } from '@angular/common/http';

describe('ParameterContextListingEffects', () => {
    interface SetupOptions {
        updateRequest?: ParameterContextUpdateRequestEntity | null;
        updateRequestParameterContextId?: string | null;
        deleteUpdateRequestInitiated?: boolean;
        listStateOverride?: any;
    }

    let action$: ReplaySubject<Action>;

    function createMockUpdateRequest(): ParameterContextUpdateRequest {
        return {
            requestId: 'test-request-id',
            uri: 'http://localhost:8080/test-uri',
            lastUpdated: '2023-01-01T00:00:00Z',
            complete: false,
            failureReason: undefined,
            percentComponent: 50,
            state: 'In Progress',
            updateSteps: [],
            parameterContext: {} as any,
            referencingComponents: []
        };
    }

    async function setup({
        updateRequest = null,
        updateRequestParameterContextId = null,
        deleteUpdateRequestInitiated = false,
        listStateOverride
    }: SetupOptions = {}) {
        await TestBed.configureTestingModule({
            providers: [
                ParameterContextListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        parameterContexts: {
                            parameterContextListing: {
                                ...initialState,
                                ...listStateOverride,
                                updateRequestEntity: updateRequest,
                                updateRequestParameterContextId,
                                deleteUpdateRequestInitiated
                            }
                        }
                    }
                }),
                {
                    provide: ParameterContextService,
                    useValue: {
                        deleteParameterContextUpdate: jest.fn(),
                        pollParameterContextUpdate: jest.fn(),
                        getParameterContexts: jest.fn()
                    }
                },
                { provide: MatDialog, useValue: { open: jest.fn() } },
                { provide: Router, useValue: { navigate: jest.fn() } },
                {
                    provide: ErrorHelper,
                    useValue: { getErrorString: jest.fn(), handleLoadingError: jest.fn(), fullScreenError: jest.fn() }
                },
                { provide: Storage, useValue: { setItem: jest.fn() } }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ParameterContextListingEffects);
        const parameterContextService = TestBed.inject(ParameterContextService) as jest.Mocked<ParameterContextService>;
        action$ = new ReplaySubject<Action>();

        const errorHelper = TestBed.inject(ErrorHelper);
        return { effects, parameterContextService, errorHelper };
    }

    beforeEach(() => {
        jest.clearAllMocks();
    });

    afterEach(() => {
        if (action$) {
            action$.complete();
        }
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('loadParameterContexts$', () => {
        it('loads successfully', async () => {
            const { effects, parameterContextService } = await setup();

            action$.next(ParameterContextListingActions.loadParameterContexts());

            (parameterContextService.getParameterContexts as jest.Mock).mockReturnValueOnce(
                of({ parameterContexts: [], currentTime: 't' })
            );

            const result = await new Promise((resolve) =>
                effects.loadParameterContexts$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ParameterContextListingActions.loadParameterContextsSuccess({
                    response: { parameterContexts: [], loadedTimestamp: 't' }
                })
            );
        });

        it('errors on initial load (hasExistingData=false)', async () => {
            const { effects, parameterContextService } = await setup();

            action$.next(ParameterContextListingActions.loadParameterContexts());

            const error = new HttpErrorResponse({ status: 500 });
            (parameterContextService.getParameterContexts as jest.Mock).mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadParameterContexts$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('errors on refresh (hasExistingData=true)', async () => {
            const stateWithData = { loadedTimestamp: 'prev' };
            const { effects, parameterContextService } = await setup({ listStateOverride: stateWithData });

            action$.next(ParameterContextListingActions.loadParameterContexts());

            const error = new HttpErrorResponse({ status: 500 });
            (parameterContextService.getParameterContexts as jest.Mock).mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadParameterContexts$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('loadParameterContextsError$', () => {
        it('should handle parameter contexts error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ParameterContextListingActions.parameterContextListingBannerApiError({
                error: 'Error message'
            });
            action$.next(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadParameterContextsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle parameter contexts error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ParameterContextListingActions.parameterContextListingBannerApiError({
                error: 'Error message'
            });
            action$.next(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: 'prev',
                    status: 'success'
                })
            );
            jest.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadParameterContextsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });

    describe('stopPollingParameterContextUpdateRequest$', () => {
        it('should dispatch deleteParameterContextUpdateRequest when triggered', async () => {
            const { effects } = await setup();

            action$.next(ParameterContextListingActions.stopPollingParameterContextUpdateRequest());

            effects.stopPollingParameterContextUpdateRequest$.subscribe((action) => {
                expect(action).toEqual(ParameterContextListingActions.deleteParameterContextUpdateRequest());
            });
        });
    });

    describe('deleteParameterContextUpdateRequest$', () => {
        it('should call service when deleteUpdateRequestInitiated is false', async () => {
            const mockUpdateRequest = createMockUpdateRequest();
            const mockResponse = { request: mockUpdateRequest };
            const parameterContextId = 'test-parameter-context-id';

            const { effects, parameterContextService } = await setup({
                updateRequest: { request: mockUpdateRequest, parameterContextRevision: { version: 1 } },
                updateRequestParameterContextId: parameterContextId,
                deleteUpdateRequestInitiated: false
            });

            parameterContextService.deleteParameterContextUpdate.mockReturnValue(of(mockResponse));

            action$.next(ParameterContextListingActions.deleteParameterContextUpdateRequest());

            effects.deleteParameterContextUpdateRequest$.subscribe(() => {
                expect(parameterContextService.deleteParameterContextUpdate).toHaveBeenCalledWith(
                    parameterContextId,
                    mockUpdateRequest.requestId
                );
            });
        });

        it('should call service when deleteUpdateRequestInitiated is true', async () => {
            const mockUpdateRequest = createMockUpdateRequest();
            const mockResponse = { request: mockUpdateRequest };
            const parameterContextId = 'test-parameter-context-id';

            const { effects, parameterContextService } = await setup({
                updateRequest: { request: mockUpdateRequest, parameterContextRevision: { version: 1 } },
                updateRequestParameterContextId: parameterContextId,
                deleteUpdateRequestInitiated: true
            });

            parameterContextService.deleteParameterContextUpdate.mockReturnValue(of(mockResponse));

            action$.next(ParameterContextListingActions.deleteParameterContextUpdateRequest());

            effects.deleteParameterContextUpdateRequest$.subscribe(() => {
                expect(parameterContextService.deleteParameterContextUpdate).toHaveBeenCalledWith(
                    parameterContextId,
                    mockUpdateRequest.requestId
                );
            });
        });
    });

    describe('pollParameterContextUpdateRequestSuccess$', () => {
        it('should dispatch stopPolling when request is complete', async () => {
            const completeUpdateRequest = createMockUpdateRequest();
            completeUpdateRequest.complete = true;

            const response = {
                requestEntity: {
                    request: completeUpdateRequest,
                    parameterContextRevision: { version: 1 }
                }
            };

            const { effects } = await setup();

            action$.next(ParameterContextListingActions.pollParameterContextUpdateRequestSuccess({ response }));

            effects.pollParameterContextUpdateRequestSuccess$.subscribe((action) => {
                expect(action).toEqual(ParameterContextListingActions.stopPollingParameterContextUpdateRequest());
            });
        });

        it('should not dispatch when request is incomplete', async () => {
            const incompleteUpdateRequest = createMockUpdateRequest();
            incompleteUpdateRequest.complete = false;

            const response = {
                requestEntity: {
                    request: incompleteUpdateRequest,
                    parameterContextRevision: { version: 1 }
                }
            };

            const { effects } = await setup();

            const emissions: any[] = [];
            effects.pollParameterContextUpdateRequestSuccess$.subscribe((action) => {
                emissions.push(action);
            });

            action$.next(ParameterContextListingActions.pollParameterContextUpdateRequestSuccess({ response }));

            // Since the effect is synchronous with filter, we can check immediately
            expect(emissions).toEqual([]);
        });
    });
});
