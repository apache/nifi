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

import { NiFiCommon, ProcessGroupDrainSummary } from '@nifi/shared';

export const DRAINING_DESCRIPTION = 'This Process Group is being drained.';

export const DRAIN_WAITING_FOR_SOURCES = 'Waiting for source components to stop.';

export const DRAIN_NO_FLOWFILES_REMAIN = 'No FlowFiles remain.';

/**
 * Percent of the original queue that has already drained.
 */
export function getDrainPercentCompleted(originalCount: number, currentCount: number): number {
    if (originalCount === 0) {
        return 100;
    }
    return Math.min(Math.max(Math.round(((originalCount - currentCount) * 100) / originalCount), 0), 100);
}

export function getDrainRemainingText(
    summary: Pick<ProcessGroupDrainSummary, 'currentCount' | 'currentSize' | 'originalCount'>,
    nifiCommon: NiFiCommon
): string {
    const currentCount = nifiCommon.formatInteger(summary.currentCount);
    const currentSize = nifiCommon.formatDataSize(summary.currentSize);
    const originalCount = nifiCommon.formatInteger(summary.originalCount);
    return `${currentCount} FlowFiles (${currentSize}) remaining of ${originalCount}`;
}

/**
 * One-sentence progress detail shared by the canvas drain tooltip and the operate panel tooltip.
 */
export function getDrainStatusDetail(summary: ProcessGroupDrainSummary, nifiCommon: NiFiCommon): string {
    if (!summary.prepared) {
        return DRAIN_WAITING_FOR_SOURCES;
    }
    if (summary.currentCount === 0) {
        return DRAIN_NO_FLOWFILES_REMAIN;
    }
    return `${getDrainRemainingText(summary, nifiCommon)}.`;
}

/**
 * Text tooltip for the operate panel Draining badge.
 */
export function getDrainingTooltip(summary: ProcessGroupDrainSummary | null, nifiCommon: NiFiCommon): string {
    if (!summary) {
        return DRAINING_DESCRIPTION;
    }
    return `${DRAINING_DESCRIPTION} ${getDrainStatusDetail(summary, nifiCommon)}`;
}

/**
 * Drain summary the operate panel reflects: the current group when nothing is selected, the
 * selected group when exactly one Process Group is selected, and none otherwise.
 */
export function getOperationDrainSummary(
    selectionCount: number,
    currentProcessGroupDrainSummary: ProcessGroupDrainSummary | null | undefined,
    selectedProcessGroupDrainSummary: ProcessGroupDrainSummary | null | undefined
): ProcessGroupDrainSummary | null {
    if (selectionCount === 0) {
        return currentProcessGroupDrainSummary ?? null;
    }
    if (selectionCount === 1) {
        return selectedProcessGroupDrainSummary ?? null;
    }
    return null;
}
