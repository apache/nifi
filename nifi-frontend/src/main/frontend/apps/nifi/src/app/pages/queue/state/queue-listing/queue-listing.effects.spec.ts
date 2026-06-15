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
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { HttpErrorResponse } from '@angular/common/http';
import { firstValueFrom, Observable, of, Subject, throwError } from 'rxjs';
import { QueueListingEffects } from './queue-listing.effects';
import {
    pollQueueListingRequest,
    queueListingApiError,
    stopPollingQueueListingRequest,
    submitQueueListingRequest
} from './queue-listing.actions';
import { selectActiveListingRequest, selectSelectedConnection } from './queue-listing.selectors';
import { QueueService } from '../../service/queue.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { NiFiCommon } from '@nifi/shared';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';
import { ListingRequest } from './index';

function createListingRequest(overrides: Partial<ListingRequest> = {}): ListingRequest {
    return {
        id: 'listing-1',
        uri: 'http://example/listing-requests/listing-1',
        submissionTime: '2026-01-01T00:00:00Z',
        lastUpdated: '2026-01-01T00:00:01Z',
        percentCompleted: 0,
        finished: false,
        failureReason: '',
        maxResults: 100,
        sourceRunning: true,
        destinationRunning: true,
        state: 'RUNNING',
        queueSize: { byteCount: 0, objectCount: 0 },
        flowFileSummaries: [],
        ...overrides
    };
}

describe('QueueListingEffects', () => {
    async function setup(
        options: {
            selectedConnection?: { id: string; label: string } | null;
            activeListingRequest?: ListingRequest | null;
            queueService?: Partial<QueueService>;
        } = {}
    ) {
        let actions$: Observable<Action>;

        const exit$ = new Subject<void>();
        const mockDialog = {
            open: vi.fn().mockReturnValue({
                componentInstance: {
                    exit: exit$.asObservable()
                }
            }),
            closeAll: vi.fn()
        };

        await TestBed.configureTestingModule({
            providers: [
                QueueListingEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {},
                    selectors: [
                        {
                            selector: selectSelectedConnection,
                            value:
                                options.selectedConnection !== undefined
                                    ? options.selectedConnection
                                    : { id: 'conn-1', label: 'Connection' }
                        },
                        {
                            selector: selectActiveListingRequest,
                            value:
                                options.activeListingRequest !== undefined
                                    ? options.activeListingRequest
                                    : createListingRequest()
                        }
                    ]
                }),
                { provide: MatDialog, useValue: mockDialog },
                {
                    provide: QueueService,
                    useValue: {
                        submitQueueListingRequest: vi.fn(),
                        pollQueueListingRequest: vi.fn(),
                        ...options.queueService
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        getErrorString: vi.fn().mockReturnValue('queue listing failed')
                    }
                },
                {
                    provide: NiFiCommon,
                    useValue: {
                        isBlank: (value: string) => !value?.trim()
                    }
                }
            ]
        }).compileComponents();

        const effects = TestBed.inject(QueueListingEffects);
        const store = TestBed.inject(MockStore);

        return {
            effects,
            store,
            mockDialog,
            actions$: (stream: Observable<Action>) => {
                actions$ = stream;
            }
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('queueListingApiError$', () => {
        it('should close dialogs, stop polling, and surface a queue banner error', async () => {
            const { effects, store, mockDialog, actions$ } = await setup();
            actions$(of(queueListingApiError({ error: 'listing failed' })));
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            const action = await firstValueFrom(effects.queueListingApiError$);

            expect(mockDialog.closeAll).toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalledWith(stopPollingQueueListingRequest());
            expect(action).toEqual(
                ErrorActions.addBannerError({
                    errorContext: { errors: ['listing failed'], context: ErrorContextKey.QUEUE }
                })
            );
        });
    });

    describe('submitQueueListingRequest$', () => {
        it('should dispatch queueListingApiError when submit fails', async () => {
            const errorResponse = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
            const { effects, actions$ } = await setup({
                queueService: {
                    submitQueueListingRequest: vi.fn().mockReturnValue(throwError(() => errorResponse))
                }
            });
            actions$(of(submitQueueListingRequest({ request: { connectionId: 'conn-1' } })));

            const action = await firstValueFrom(effects.submitQueueListingRequest$);

            expect(action).toEqual(
                queueListingApiError({
                    error: 'queue listing failed'
                })
            );
        });
    });

    describe('pollQueueListingRequest$', () => {
        it('should dispatch queueListingApiError when poll fails', async () => {
            const errorResponse = new HttpErrorResponse({ status: 503, statusText: 'Service Unavailable' });
            const pollQueueListingRequestSpy = vi.fn().mockReturnValue(throwError(() => errorResponse));
            const { effects, actions$ } = await setup({
                queueService: {
                    pollQueueListingRequest: pollQueueListingRequestSpy
                }
            });
            actions$(of(pollQueueListingRequest()));

            const action = await firstValueFrom(effects.pollQueueListingRequest$);

            expect(pollQueueListingRequestSpy).toHaveBeenCalledWith('conn-1', 'listing-1');
            expect(action).toEqual(
                queueListingApiError({
                    error: 'queue listing failed'
                })
            );
        });
    });
});
