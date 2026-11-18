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

import { NiFiCommon } from '@nifi/shared';
import { createProcessGroupDrainSummary } from '../../../testing/process-group-drain-fixture';
import {
    DRAIN_NO_FLOWFILES_REMAIN,
    DRAIN_WAITING_FOR_SOURCES,
    DRAINING_DESCRIPTION,
    getDrainingTooltip,
    getDrainPercentCompleted,
    getDrainStatusDetail,
    getOperationDrainSummary
} from './drain-summary.utils';

describe('getDrainPercentCompleted', () => {
    it('is 100 when there was nothing queued', () => {
        expect(getDrainPercentCompleted(0, 0)).toBe(100);
    });

    it('is the drained share of the original count', () => {
        expect(getDrainPercentCompleted(10, 4)).toBe(60);
    });

    it('is clamped when the queue grew past the original count', () => {
        expect(getDrainPercentCompleted(10, 15)).toBe(0);
    });
});

describe('getDrainStatusDetail', () => {
    const nifiCommon = new NiFiCommon();

    it('explains that source components are still stopping', () => {
        expect(getDrainStatusDetail(createProcessGroupDrainSummary({ prepared: false }), nifiCommon)).toBe(
            DRAIN_WAITING_FOR_SOURCES
        );
    });

    it('shows remaining FlowFiles after the drain is prepared', () => {
        const summary = createProcessGroupDrainSummary({
            prepared: true,
            originalCount: 10,
            originalSize: 1024,
            currentCount: 4,
            currentSize: 400
        });

        expect(getDrainStatusDetail(summary, nifiCommon)).toBe(
            `${nifiCommon.formatInteger(4)} FlowFiles (${nifiCommon.formatDataSize(400)}) remaining of ${nifiCommon.formatInteger(10)}.`
        );
    });

    it('says no FlowFiles remain when the queue is empty', () => {
        expect(
            getDrainStatusDetail(createProcessGroupDrainSummary({ prepared: true, currentCount: 0 }), nifiCommon)
        ).toBe(DRAIN_NO_FLOWFILES_REMAIN);
    });
});

describe('getDrainingTooltip', () => {
    const nifiCommon = new NiFiCommon();

    it('is the description alone without a summary', () => {
        expect(getDrainingTooltip(null, nifiCommon)).toBe(DRAINING_DESCRIPTION);
    });

    it('appends the status detail to the description', () => {
        expect(getDrainingTooltip(createProcessGroupDrainSummary({ prepared: false }), nifiCommon)).toBe(
            `${DRAINING_DESCRIPTION} ${DRAIN_WAITING_FOR_SOURCES}`
        );
    });
});

describe('getOperationDrainSummary', () => {
    const current = createProcessGroupDrainSummary({ id: 'current' });
    const selected = createProcessGroupDrainSummary({ id: 'selected' });

    it('uses the current group when nothing is selected', () => {
        expect(getOperationDrainSummary(0, current, selected)).toBe(current);
    });

    it('uses the selected group when one component is selected', () => {
        expect(getOperationDrainSummary(1, current, selected)).toBe(selected);
        expect(getOperationDrainSummary(1, current, undefined)).toBeNull();
    });

    it('is null for a multi-selection', () => {
        expect(getOperationDrainSummary(2, current, selected)).toBeNull();
    });
});
