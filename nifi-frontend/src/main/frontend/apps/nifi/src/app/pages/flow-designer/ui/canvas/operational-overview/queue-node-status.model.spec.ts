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

import { observeThreshold, thresholdBytes, queueNodeRows, QueueStatusResponse } from './queue-node-status.model';

describe('queue threshold observations', () => {
    it.each([
        ['1 KB', 1024],
        ['1.5 MB', 1572864],
        ['2GB', 2147483648],
        ['0 bytes', 0],
        ['3 b', 3]
    ])('parses %s with NiFi binary units', (text, expected) => {
        expect(thresholdBytes(text as string)).toBe(expected);
    });
    it('does not guess unsupported, negative, fractional-byte, or unsafe thresholds', () => {
        for (const text of [undefined, '', '100', '-1 KB', '1.1 B', '1 XB', '99999999999 PB'])
            expect(thresholdBytes(text)).toBeUndefined();
    });
    it('compares raw observations rather than rounded reported utilization', () => {
        expect(observeThreshold(9999, 10000).state).toBe('below');
        expect(observeThreshold(10000, 10000).state).toBe('at-or-above');
        expect(observeThreshold(12000, 10000).percent).toBe(120);
        expect(observeThreshold(0, 10000).percent).toBe(0);
    });
    it('distinguishes disabled thresholds, unknown values and zero', () => {
        expect(observeThreshold(undefined, 0).state).toBe('disabled');
        expect(observeThreshold(undefined, 10).state).toBe('unavailable');
        expect(observeThreshold(0, 10).state).toBe('below');
        expect(observeThreshold(10, undefined).state).toBe('unavailable');
        for (const bad of [Number.NaN, Infinity, -1, Number.MAX_SAFE_INTEGER + 1])
            expect(observeThreshold(bad, 10).state).toBe('unavailable');
    });
    it('keeps skewed nodes separate and never divides cluster totals by one threshold', () => {
        const response: QueueStatusResponse = {
            canRead: true,
            connectionStatus: {
                aggregateSnapshot: { flowFilesQueued: 150 },
                nodeSnapshots: [
                    { nodeId: 'a', statusSnapshot: { flowFilesQueued: 110, bytesQueued: 0, percentUseCount: 100 } },
                    { nodeId: 'b', statusSnapshot: { flowFilesQueued: 40, bytesQueued: 1024, percentUseCount: 40 } }
                ]
            }
        };
        const rows = queueNodeRows(response, true, 100, '1 KB');
        expect(rows.map((row) => row.count.state)).toEqual(['at-or-above', 'below']);
        expect(rows.map((row) => row.bytes.state)).toEqual(['below', 'at-or-above']);
        expect(rows.map((row) => row.count.used)).toEqual([110, 40]);
    });
    it('does not reinterpret aggregate-only cluster status or unknown topology as a node', () => {
        const response = { canRead: true, connectionStatus: { aggregateSnapshot: { flowFilesQueued: 150 } } };
        expect(queueNodeRows(response, true, 100, '1 KB')).toEqual([]);
        expect(queueNodeRows(response, undefined, 100, '1 KB')).toEqual([]);
        expect(queueNodeRows(response, false, 100, '1 KB')[0].count.state).toBe('at-or-above');
    });
    it('does not turn missing node snapshots or restricted data into healthy rows', () => {
        const response = { canRead: true, connectionStatus: { nodeSnapshots: [{ nodeId: 'missing' }] } };
        expect(queueNodeRows(response, true, 100, '1 KB')[0].count.state).toBe('unavailable');
        expect(queueNodeRows({ ...response, canRead: false }, true, 100, '1 KB')).toEqual([]);
    });
});
