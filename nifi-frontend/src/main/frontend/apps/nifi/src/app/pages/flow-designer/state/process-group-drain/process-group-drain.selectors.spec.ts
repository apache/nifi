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

import { DrainProcessGroupState, processGroupDrainFeatureKey } from './index';
import {
    selectDrainDialogState,
    selectDrainProcessGroupId,
    selectDrainProcessGroupState,
    selectDrainRequest,
    selectDrainRequestId,
    selectDrainStatus
} from './process-group-drain.selectors';
import { createProcessGroupDrainRequest } from '../../../../testing/process-group-drain-fixture';
import { canvasFeatureKey } from '../index';

describe('process-group-drain selectors', () => {
    const drainRequest = createProcessGroupDrainRequest();
    const drainState: DrainProcessGroupState = {
        processGroupId: drainRequest.processGroupId,
        drainRequestId: drainRequest.id,
        drainRequest,
        status: 'draining',
        error: null
    };
    const rootState = {
        [canvasFeatureKey]: {
            [processGroupDrainFeatureKey]: drainState
        }
    };

    it('selects the drain slice from canvas state', () => {
        expect(selectDrainProcessGroupState(rootState as never)).toEqual(drainState);
    });

    it('selects the process group id', () => {
        expect(selectDrainProcessGroupId(rootState as never)).toBe('pg-1');
    });

    it('selects the drain request id', () => {
        expect(selectDrainRequestId(rootState as never)).toBe('drain-1');
    });

    it('selects the drain request', () => {
        expect(selectDrainRequest(rootState as never)).toEqual(drainState.drainRequest);
    });

    it('selects the drain status', () => {
        expect(selectDrainStatus(rootState as never)).toBe('draining');
    });

    it('selects only the status and drain request for the dialog', () => {
        expect(selectDrainDialogState(rootState as never)).toEqual({
            status: drainState.status,
            drainRequest: drainState.drainRequest
        });
    });
});
