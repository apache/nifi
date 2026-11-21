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

import { initialState, processGroupDrainReducer } from './process-group-drain.reducer';
import {
    beginDrain,
    beginDrainSuccess,
    cancelDrain,
    cancelDrainSuccess,
    drainApiError,
    drainEnded,
    pollDrainSuccess,
    resetDrainState,
    viewDrain
} from './process-group-drain.actions';
import {
    createProcessGroupDrainRequest,
    createProcessGroupDrainRequestEntity
} from '../../../../testing/process-group-drain-fixture';

describe('processGroupDrainReducer', () => {
    it('records the process group and starting status on beginDrain', () => {
        const next = processGroupDrainReducer(initialState, beginDrain({ request: { processGroupId: 'pg-1' } }));

        expect(next.processGroupId).toBe('pg-1');
        expect(next.status).toBe('starting');
        expect(next.drainRequest).toBeNull();
    });

    it('records the drain id on viewDrain', () => {
        const next = processGroupDrainReducer(
            initialState,
            viewDrain({ request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' } })
        );

        expect(next.processGroupId).toBe('pg-1');
        expect(next.drainRequestId).toBe('drain-1');
        expect(next.status).toBe('draining');
    });

    it('stores the drain request after beginDrainSuccess', () => {
        const entity = createProcessGroupDrainRequestEntity();
        const starting = processGroupDrainReducer(initialState, beginDrain({ request: { processGroupId: 'pg-1' } }));
        const next = processGroupDrainReducer(
            starting,
            beginDrainSuccess({ response: { drainRequestEntity: entity } })
        );

        expect(next.drainRequestId).toBe('drain-1');
        expect(next.drainRequest).toEqual(entity.drainRequest);
        expect(next.status).toBe('draining');
    });

    it('keeps the drain id when a poll entity comes back empty', () => {
        const draining = processGroupDrainReducer(
            initialState,
            beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        const next = processGroupDrainReducer(
            draining,
            pollDrainSuccess({ response: { drainRequestEntity: createProcessGroupDrainRequestEntity(null) } })
        );

        expect(next.drainRequestId).toBe('drain-1');
        expect(next.drainRequest).toBeNull();
        expect(next.status).toBe('draining');
    });

    it('sets ending status on cancelDrain', () => {
        const draining = processGroupDrainReducer(
            initialState,
            beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        const next = processGroupDrainReducer(draining, cancelDrain());

        expect(next.status).toBe('ending');
        expect(next.drainRequestId).toBe('drain-1');
    });

    it('stores the final drain request on cancelDrainSuccess', () => {
        const ending = processGroupDrainReducer(
            processGroupDrainReducer(
                initialState,
                beginDrainSuccess({
                    response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
                })
            ),
            cancelDrain()
        );
        const next = processGroupDrainReducer(
            ending,
            cancelDrainSuccess({
                response: {
                    drainRequestEntity: createProcessGroupDrainRequestEntity({
                        state: 'ENDED',
                        currentCount: 2
                    })
                }
            })
        );

        expect(next.status).toBe('ended');
        expect(next.drainRequest?.state).toBe('ENDED');
        expect(next.drainRequest?.currentCount).toBe(2);
    });

    it('marks the drain ended without clearing the last request', () => {
        const draining = processGroupDrainReducer(
            initialState,
            beginDrainSuccess({
                response: {
                    drainRequestEntity: createProcessGroupDrainRequestEntity({
                        state: 'ENDED',
                        currentCount: 0
                    })
                }
            })
        );
        const next = processGroupDrainReducer(draining, drainEnded());

        expect(next.status).toBe('ended');
        expect(next.drainRequest).toEqual(createProcessGroupDrainRequest({ state: 'ENDED', currentCount: 0 }));
    });

    it('resets state and records the error on drainApiError', () => {
        const draining = processGroupDrainReducer(
            initialState,
            beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );
        const next = processGroupDrainReducer(draining, drainApiError({ error: 'Access is denied' }));

        expect(next).toEqual({
            ...initialState,
            error: 'Access is denied'
        });
    });

    it('returns to the initial state on resetDrainState', () => {
        const draining = processGroupDrainReducer(
            initialState,
            beginDrainSuccess({
                response: { drainRequestEntity: createProcessGroupDrainRequestEntity() }
            })
        );

        expect(processGroupDrainReducer(draining, resetDrainState())).toEqual(initialState);
    });
});
