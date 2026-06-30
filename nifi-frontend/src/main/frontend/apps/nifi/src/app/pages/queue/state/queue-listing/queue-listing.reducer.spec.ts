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

import { queueListingReducer, initialState } from './queue-listing.reducer';
import {
    submitQueueListingRequest,
    stopPollingQueueListingRequest,
    deleteQueueListingRequestSuccess,
    queueListingApiError,
    resetQueueListingState
} from './queue-listing.actions';
import { QueueListingState } from './index';

describe('queueListingReducer', () => {
    describe('submitQueueListingRequest', () => {
        it('should set status to loading', () => {
            const next = queueListingReducer(
                initialState,
                submitQueueListingRequest({ request: { connectionId: 'conn-1' } as any })
            );

            expect(next.status).toBe('loading');
        });
    });

    describe('stopPollingQueueListingRequest', () => {
        it('resets status to success when dispatched while loading (cancel flow)', () => {
            const loadingState: QueueListingState = { ...initialState, status: 'loading' };

            const next = queueListingReducer(loadingState, stopPollingQueueListingRequest());

            expect(next.status).toBe('success');
        });

        it('is a safe no-op when status is already success (normal completion path)', () => {
            const successState: QueueListingState = { ...initialState, status: 'success' };

            const next = queueListingReducer(successState, stopPollingQueueListingRequest());

            expect(next.status).toBe('success');
        });
    });

    describe('deleteQueueListingRequestSuccess', () => {
        it('should clear the activeListingRequest', () => {
            const stateWithActive: QueueListingState = {
                ...initialState,
                activeListingRequest: { id: 'req-1' } as any
            };

            const next = queueListingReducer(stateWithActive, deleteQueueListingRequestSuccess());

            expect(next.activeListingRequest).toBeNull();
        });
    });

    describe('queueListingApiError', () => {
        it('should set status to error', () => {
            const next = queueListingReducer(initialState, queueListingApiError({ error: 'oops' }));

            expect(next.status).toBe('error');
        });
    });

    describe('resetQueueListingState', () => {
        it('should reset to the initial state', () => {
            const loadingState: QueueListingState = { ...initialState, status: 'loading' };

            const next = queueListingReducer(loadingState, resetQueueListingState());

            expect(next).toEqual(initialState);
        });
    });
});
