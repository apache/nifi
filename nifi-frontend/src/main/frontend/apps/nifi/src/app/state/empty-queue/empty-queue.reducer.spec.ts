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

import { emptyQueueReducer, initialState } from './empty-queue.reducer';
import {
    pollEmptyQueueRequestSuccess,
    resetEmptyQueueState,
    submitEmptyQueueRequest,
    submitEmptyQueueRequestSuccess,
    submitEmptyQueuesRequest,
    submitEmptyQueuesRequestSuccess
} from './empty-queue.actions';
import { DropRequestEntity } from './index';

function createDropEntity(overrides: Partial<DropRequestEntity['dropRequest']> = {}): DropRequestEntity {
    return {
        dropRequest: {
            id: 'drop-1',
            uri: 'http://example/drop-requests/drop-1',
            submissionTime: '2023-01-01T00:00:00Z',
            lastUpdated: '2023-01-01T00:00:01Z',
            percentCompleted: 100,
            finished: true,
            failureReason: '',
            currentCount: 0,
            currentSize: 0,
            current: '0 / 0 bytes',
            originalCount: 5,
            originalSize: 100,
            original: '5 / 100 bytes',
            droppedCount: 5,
            droppedSize: 100,
            dropped: '5 / 100 bytes',
            state: 'COMPLETE',
            ...overrides
        }
    };
}

describe('emptyQueueReducer', () => {
    it('should record the connectionId and source on submitEmptyQueueRequest', () => {
        const next = emptyQueueReducer(
            initialState,
            submitEmptyQueueRequest({
                request: { connectionId: 'conn-1', source: 'connector-canvas' }
            })
        );

        expect(next.connectionId).toBe('conn-1');
        expect(next.source).toBe('connector-canvas');
        expect(next.status).toBe('loading');
    });

    it('should record the processGroupId and source on submitEmptyQueuesRequest', () => {
        const next = emptyQueueReducer(
            initialState,
            submitEmptyQueuesRequest({
                request: { processGroupId: 'pg-1', source: 'flow-designer' }
            })
        );

        expect(next.processGroupId).toBe('pg-1');
        expect(next.source).toBe('flow-designer');
        expect(next.status).toBe('loading');
    });

    it('should set status to success and persist the loadedTimestamp on submitEmptyQueueRequestSuccess', () => {
        const dropEntity = createDropEntity({ lastUpdated: '2023-02-01T12:00:00Z' });

        const next = emptyQueueReducer(initialState, submitEmptyQueueRequestSuccess({ response: { dropEntity } }));

        expect(next.status).toBe('success');
        expect(next.dropEntity).toEqual(dropEntity);
        expect(next.loadedTimestamp).toBe('2023-02-01T12:00:00Z');
    });

    it('should set status to success and persist the loadedTimestamp on submitEmptyQueuesRequestSuccess', () => {
        const dropEntity = createDropEntity({ lastUpdated: '2023-02-15T08:00:00Z' });

        const next = emptyQueueReducer(initialState, submitEmptyQueuesRequestSuccess({ response: { dropEntity } }));

        expect(next.status).toBe('success');
        expect(next.dropEntity).toEqual(dropEntity);
        expect(next.loadedTimestamp).toBe('2023-02-15T08:00:00Z');
    });

    it('should preserve the source through the request lifecycle', () => {
        const submitted = emptyQueueReducer(
            initialState,
            submitEmptyQueueRequest({
                request: { connectionId: 'conn-1', source: 'connector-canvas' }
            })
        );

        const polled = emptyQueueReducer(
            submitted,
            pollEmptyQueueRequestSuccess({ response: { dropEntity: createDropEntity() } })
        );

        expect(polled.source).toBe('connector-canvas');
    });

    it('should clear a previously set processGroupId when starting a new single-connection request', () => {
        const afterBulk = emptyQueueReducer(
            initialState,
            submitEmptyQueuesRequest({
                request: { processGroupId: 'pg-1', source: 'flow-designer' }
            })
        );

        const next = emptyQueueReducer(
            afterBulk,
            submitEmptyQueueRequest({
                request: { connectionId: 'conn-1', source: 'connector-canvas' }
            })
        );

        expect(next.connectionId).toBe('conn-1');
        expect(next.processGroupId).toBeNull();
        expect(next.source).toBe('connector-canvas');
        expect(next.status).toBe('loading');
    });

    it('should clear a previously set connectionId when starting a new bulk request', () => {
        const afterSingle = emptyQueueReducer(
            initialState,
            submitEmptyQueueRequest({
                request: { connectionId: 'conn-1', source: 'connector-canvas' }
            })
        );

        const next = emptyQueueReducer(
            afterSingle,
            submitEmptyQueuesRequest({
                request: { processGroupId: 'pg-1', source: 'flow-designer' }
            })
        );

        expect(next.processGroupId).toBe('pg-1');
        expect(next.connectionId).toBeNull();
        expect(next.source).toBe('flow-designer');
        expect(next.status).toBe('loading');
    });

    it('should reset to the initial state on resetEmptyQueueState', () => {
        const submitted = emptyQueueReducer(
            initialState,
            submitEmptyQueuesRequest({
                request: { processGroupId: 'pg-1', source: 'flow-designer' }
            })
        );

        const reset = emptyQueueReducer(submitted, resetEmptyQueueState());

        expect(reset).toEqual(initialState);
        expect(reset.source).toBeNull();
    });
});
