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

import { connectorProvenancePreviewReducer, initialState } from './connector-provenance-preview.reducer';
import {
    loadError,
    loadLatestEventsForComponent,
    loadLatestEventsForComponentSuccess,
    resetState
} from './connector-provenance-preview.actions';
import { ProvenanceEvent } from '../../../../state/shared';

describe('connectorProvenancePreviewReducer', () => {
    const SAMPLE_EVENT = { id: 'evt-1', eventId: 1, eventType: 'RECEIVE' } as ProvenanceEvent;

    it('should set status to loading and clear events / error on loadLatestEventsForComponent', () => {
        const seeded = {
            events: [SAMPLE_EVENT],
            error: 'previous',
            status: 'error' as const
        };
        const next = connectorProvenancePreviewReducer(seeded, loadLatestEventsForComponent({ componentId: 'p' }));

        expect(next.status).toBe('loading');
        expect(next.events).toEqual([]);
        expect(next.error).toBeNull();
    });

    it('should populate events and set status to success on loadLatestEventsForComponentSuccess', () => {
        const events = [SAMPLE_EVENT];
        const next = connectorProvenancePreviewReducer(initialState, loadLatestEventsForComponentSuccess({ events }));

        expect(next.status).toBe('success');
        expect(next.events).toEqual(events);
    });

    it('should set status to error and capture error message on loadError', () => {
        const next = connectorProvenancePreviewReducer(initialState, loadError({ error: 'boom' }));

        expect(next.status).toBe('error');
        expect(next.error).toBe('boom');
    });

    it('should reset to initial state on resetState', () => {
        const dirty = {
            events: [SAMPLE_EVENT],
            error: 'something',
            status: 'success' as const
        };
        const next = connectorProvenancePreviewReducer(dirty, resetState());

        expect(next).toEqual(initialState);
    });
});
