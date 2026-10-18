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

export interface QueueSnapshot {
    flowFilesQueued?: number;
    bytesQueued?: number;
    percentUseCount?: number;
    percentUseBytes?: number;
}
export interface QueueStatusResponse {
    canRead?: boolean;
    connectionStatus?: {
        id?: string;
        statsLastRefreshed?: string;
        aggregateSnapshot?: QueueSnapshot;
        nodeSnapshots?: { nodeId?: string; address?: string; apiPort?: number; statusSnapshot?: QueueSnapshot }[];
    };
}
export interface ThresholdObservation {
    state: 'unavailable' | 'disabled' | 'below' | 'at-or-above';
    used?: number;
    limit?: number;
    percent?: number;
}
export interface QueueNodeRow {
    id: string;
    name: string;
    count: ThresholdObservation;
    bytes: ThresholdObservation;
}

// The existing NiFi display units use binary multiples (1024). Restrict parsing
// to canonical units; unfamiliar configuration must remain unavailable.
export function thresholdBytes(value?: string): number | undefined {
    const match = value?.trim().match(/^(\d+(?:\.\d+)?)\s*(B|bytes?|KB|MB|GB|TB|PB)$/i);
    if (!match) return undefined;
    const unit = match[2].toUpperCase();
    const exponent = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'].indexOf(unit.startsWith('BYTE') ? 'B' : unit);
    const bytes = Number(match[1]) * 1024 ** exponent;
    return Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : undefined;
}

export function observeThreshold(used?: number, limit?: number): ThresholdObservation {
    const valid = (value: unknown): value is number =>
        typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
    const measured = valid(used) ? used : undefined;
    if (!valid(limit)) return { state: 'unavailable', used: measured };
    if (limit === 0) return { state: 'disabled', used: measured, limit };
    if (measured === undefined) return { state: 'unavailable', limit };
    return {
        state: measured >= limit ? 'at-or-above' : 'below',
        used: measured,
        limit,
        percent: (measured / limit) * 100
    };
}

export function queueNodeRows(
    response: QueueStatusResponse,
    clustered: boolean | undefined,
    countLimit?: number,
    byteLimit?: string
): QueueNodeRow[] {
    if (response.canRead !== true) return [];
    const status = response.connectionStatus;
    const rows = status?.nodeSnapshots?.length
        ? status.nodeSnapshots.map((node, index) => ({
              id: node.nodeId || `reported-${index}`,
              name: node.address
                  ? `${node.address}${node.apiPort === undefined ? '' : ':' + node.apiPort}`
                  : node.nodeId || 'Unnamed node',
              snapshot: node.statusSnapshot
          }))
        : clustered === false && status?.aggregateSnapshot
          ? [{ id: 'standalone', name: 'This instance', snapshot: status.aggregateSnapshot }]
          : [];
    return rows.map((row) => ({
        id: row.id,
        name: row.name,
        count: observeThreshold(row.snapshot?.flowFilesQueued, countLimit),
        bytes: observeThreshold(row.snapshot?.bytesQueued, thresholdBytes(byteLimit))
    }));
}
