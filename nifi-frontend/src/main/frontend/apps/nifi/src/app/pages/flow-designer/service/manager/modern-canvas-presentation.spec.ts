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

import { processorRunStatus, reportedQueueUtilization } from './modern-canvas-presentation';

describe('modern canvas presentation', () => {
    it.each(['Running', 'Stopped', 'Invalid', 'Validating', 'Disabled'])(
        'preserves the reported %s state',
        (status) => {
            expect(processorRunStatus(status)).toBe(status);
        }
    );
    it('does not invent a stopped or healthy state for missing or unknown status', () => {
        for (const status of [undefined, '', 'FutureState'])
            expect(processorRunStatus(status)).toBe('Status unavailable');
    });
    it('uses the highest reported utilization across count and bytes, including a reported zero', () => {
        expect(reportedQueueUtilization({ percentUseCount: 12, percentUseBytes: 98 })).toBe(98);
        expect(reportedQueueUtilization({ percentUseCount: 100, percentUseBytes: 4 })).toBe(100);
        expect(reportedQueueUtilization({ percentUseCount: 0 })).toBe(0);
    });
    it('does not turn missing, disabled or invalid metrics into zero', () => {
        expect(reportedQueueUtilization()).toBeUndefined();
        expect(reportedQueueUtilization({})).toBeUndefined();
        expect(reportedQueueUtilization({ percentUseCount: Number.NaN, percentUseBytes: -1 })).toBeUndefined();
        expect(reportedQueueUtilization({ percentUseCount: Infinity, percentUseBytes: 3 })).toBe(3);
    });
});
