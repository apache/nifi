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

import {
    detectOverlappingConnections,
    OverlapDetectionConnection,
    wouldRemovalCauseOverlap
} from './overlap-detection.utils';

function createConnection(overrides: Partial<OverlapDetectionConnection> = {}): OverlapDetectionConnection {
    return {
        id: 'conn-1',
        sourceId: 'proc-a',
        destinationId: 'proc-b',
        sourceGroupId: 'root',
        destinationGroupId: 'root',
        bends: [],
        ...overrides
    };
}

describe('detectOverlappingConnections', () => {
    const processGroupId = 'root';

    it('should return empty array when no connections are provided', () => {
        expect(detectOverlappingConnections([], processGroupId)).toEqual([]);
    });

    it('should return empty array for null/undefined connections', () => {
        expect(detectOverlappingConnections(null as any, processGroupId)).toEqual([]);
        expect(detectOverlappingConnections(undefined as any, processGroupId)).toEqual([]);
    });

    it('should return empty array when only one connection exists between a pair', () => {
        const connections = [createConnection({ id: 'conn-1' })];
        expect(detectOverlappingConnections(connections, processGroupId)).toEqual([]);
    });

    it('should detect two straight-line connections between the same pair', () => {
        const connections = [
            createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-2', sourceId: 'proc-a', destinationId: 'proc-b' })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toHaveLength(1);
        expect(result[0].connectionIds).toContain('conn-1');
        expect(result[0].connectionIds).toContain('conn-2');
    });

    it('should not flag connections with bend points as overlapping', () => {
        const connections = [
            createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b', bends: [] }),
            createConnection({
                id: 'conn-2',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 200 }]
            })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toEqual([]);
    });

    it('should treat bidirectional connections as the same visual pair', () => {
        const connections = [
            createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-2', sourceId: 'proc-b', destinationId: 'proc-a' })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toHaveLength(1);
        expect(result[0].connectionIds).toHaveLength(2);
    });

    it('should detect multiple overlap groups independently', () => {
        const connections = [
            createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-2', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-3', sourceId: 'proc-c', destinationId: 'proc-d' }),
            createConnection({ id: 'conn-4', sourceId: 'proc-c', destinationId: 'proc-d' })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toHaveLength(2);
    });

    it('should resolve group IDs when source/destination are in different groups', () => {
        const connections = [
            createConnection({
                id: 'conn-1',
                sourceId: 'port-1',
                destinationId: 'proc-b',
                sourceGroupId: 'child-group'
            }),
            createConnection({
                id: 'conn-2',
                sourceId: 'port-2',
                destinationId: 'proc-b',
                sourceGroupId: 'child-group'
            })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toHaveLength(1);
        expect(result[0].sourceComponentId).toBeDefined();
        expect(result[0].destinationComponentId).toBeDefined();
    });

    it('should not flag connections between different component pairs', () => {
        const connections = [
            createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-2', sourceId: 'proc-a', destinationId: 'proc-c' })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toEqual([]);
    });

    it('should handle three or more overlapping connections in the same group', () => {
        const connections = [
            createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-2', sourceId: 'proc-a', destinationId: 'proc-b' }),
            createConnection({ id: 'conn-3', sourceId: 'proc-a', destinationId: 'proc-b' })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toHaveLength(1);
        expect(result[0].connectionIds).toHaveLength(3);
    });

    it('should not flag a single connection with no bends', () => {
        const connections = [createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' })];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toEqual([]);
    });

    it('should ignore connections where both have bend points', () => {
        const connections = [
            createConnection({
                id: 'conn-1',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 50 }]
            }),
            createConnection({
                id: 'conn-2',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 50 }]
            })
        ];

        const result = detectOverlappingConnections(connections, processGroupId);
        expect(result).toEqual([]);
    });
});

describe('wouldRemovalCauseOverlap', () => {
    const processGroupId = 'root';

    it('should return false when no other connections exist', () => {
        const connections = [createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' })];

        expect(wouldRemovalCauseOverlap('conn-1', connections, processGroupId)).toBe(false);
    });

    it('should return true when another straight-line connection exists between same pair', () => {
        const connections = [
            createConnection({
                id: 'conn-1',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 50 }]
            }),
            createConnection({ id: 'conn-2', sourceId: 'proc-a', destinationId: 'proc-b' })
        ];

        expect(wouldRemovalCauseOverlap('conn-1', connections, processGroupId)).toBe(true);
    });

    it('should return false when other connection has bend points', () => {
        const connections = [
            createConnection({
                id: 'conn-1',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 50 }]
            }),
            createConnection({
                id: 'conn-2',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 200, y: 75 }]
            })
        ];

        expect(wouldRemovalCauseOverlap('conn-1', connections, processGroupId)).toBe(false);
    });

    it('should return false when other connection is between different components', () => {
        const connections = [
            createConnection({
                id: 'conn-1',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 50 }]
            }),
            createConnection({ id: 'conn-2', sourceId: 'proc-a', destinationId: 'proc-c' })
        ];

        expect(wouldRemovalCauseOverlap('conn-1', connections, processGroupId)).toBe(false);
    });

    it('should return false for unknown connection ID', () => {
        const connections = [createConnection({ id: 'conn-1', sourceId: 'proc-a', destinationId: 'proc-b' })];

        expect(wouldRemovalCauseOverlap('unknown', connections, processGroupId)).toBe(false);
    });

    it('should handle bidirectional connections', () => {
        const connections = [
            createConnection({
                id: 'conn-1',
                sourceId: 'proc-a',
                destinationId: 'proc-b',
                bends: [{ x: 100, y: 50 }]
            }),
            createConnection({ id: 'conn-2', sourceId: 'proc-b', destinationId: 'proc-a' })
        ];

        expect(wouldRemovalCauseOverlap('conn-1', connections, processGroupId)).toBe(true);
    });
});
