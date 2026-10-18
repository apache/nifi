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

// Presentation only: use the server's node-max percentages, never cluster totals
// divided by a single-node threshold. Missing or invalid values remain unavailable.
export function reportedQueueUtilization(snapshot?: {
    percentUseCount?: number;
    percentUseBytes?: number;
}): number | undefined {
    const values = [snapshot?.percentUseCount, snapshot?.percentUseBytes].filter(
        (value): value is number => typeof value === 'number' && Number.isFinite(value) && value >= 0
    );
    return values.length ? Math.max(...values) : undefined;
}

export function processorRunStatus(value?: string): string {
    return value && ['Running', 'Stopped', 'Invalid', 'Validating', 'Disabled'].includes(value)
        ? value
        : 'Status unavailable';
}

export function processorVisualIdentity(type: string | undefined): { glyph: string; family: string } {
    if (!type) return { glyph: '\uf013', family: 'generic' };
    const name = type.split('.').pop() || '';
    if (/Kafka/i.test(name)) return { glyph: '\uf0ec', family: 'messaging' };
    if (/S3|Azure|GCS/i.test(name)) return { glyph: '\uf0c2', family: 'storage' };
    if (/Route|Distribute|Partition/i.test(name)) return { glyph: '\uf0e8', family: 'routing' };
    if (/PutFile|FetchFile|ListFile|GetFile/i.test(name)) return { glyph: '\uf07c', family: 'file' };
    if (/Generate|Listen|Consume/i.test(name)) return { glyph: '\uf0e7', family: 'source' };
    if (/Record|JSON|Text|Attribute/i.test(name)) return { glyph: '\uf15c', family: 'record' };
    return { glyph: '\uf013', family: 'generic' };
}
