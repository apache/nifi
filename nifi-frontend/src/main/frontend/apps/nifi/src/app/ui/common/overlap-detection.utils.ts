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

export interface OverlappingConnectionGroup {
    sourceComponentId: string;
    destinationComponentId: string;
    connectionIds: string[];
}

export interface OverlapDetectionConnection {
    id: string;
    sourceId: string;
    destinationId: string;
    sourceGroupId: string;
    destinationGroupId: string;
    bends: Array<{ x: number; y: number }>;
}

/**
 * Pure function to detect groups of overlapping connections.
 *
 * Two connections overlap when they share the same effective source and destination
 * components and both have no bend points (straight-line paths).
 *
 * @param connections       Array of connection entities
 * @param currentProcessGroupId  The current process group context for resolving component IDs
 * @returns Array of overlap groups, each containing the affected connection IDs
 */
export function detectOverlappingConnections(
    connections: OverlapDetectionConnection[],
    currentProcessGroupId: string
): OverlappingConnectionGroup[] {
    if (!connections || connections.length === 0) {
        return [];
    }

    const pairMap = new Map<string, string[]>();

    connections.forEach((connection) => {
        if (connection.bends && connection.bends.length > 0) {
            return;
        }

        const effectiveSourceId =
            connection.sourceGroupId !== currentProcessGroupId ? connection.sourceGroupId : connection.sourceId;
        const effectiveDestinationId =
            connection.destinationGroupId !== currentProcessGroupId
                ? connection.destinationGroupId
                : connection.destinationId;

        const pairKey = [effectiveSourceId, effectiveDestinationId].sort().join('::');

        if (!pairMap.has(pairKey)) {
            pairMap.set(pairKey, []);
        }
        pairMap.get(pairKey)!.push(connection.id);
    });

    const overlappingGroups: OverlappingConnectionGroup[] = [];

    pairMap.forEach((connectionIds, pairKey) => {
        if (connectionIds.length >= 2) {
            const [componentA, componentB] = pairKey.split('::');
            overlappingGroups.push({
                sourceComponentId: componentA,
                destinationComponentId: componentB,
                connectionIds
            });
        }
    });

    return overlappingGroups;
}

/**
 * Checks whether removing all bend points from a connection would cause it to
 * visually overlap with another straight-line connection between the same components.
 *
 * @param connectionId          The connection whose bend points would be removed
 * @param connections           All connections in the current view
 * @param currentProcessGroupId The current process group context
 * @returns true if removal would cause a visual overlap
 */
export function wouldRemovalCauseOverlap(
    connectionId: string,
    connections: OverlapDetectionConnection[],
    currentProcessGroupId: string
): boolean {
    if (!connections || connections.length === 0) {
        return false;
    }

    const target = connections.find((c) => c.id === connectionId);
    if (!target) {
        return false;
    }

    const effectiveSourceId = target.sourceGroupId !== currentProcessGroupId ? target.sourceGroupId : target.sourceId;
    const effectiveDestinationId =
        target.destinationGroupId !== currentProcessGroupId ? target.destinationGroupId : target.destinationId;
    const pairKey = [effectiveSourceId, effectiveDestinationId].sort().join('::');

    return connections.some((c) => {
        if (c.id === connectionId) {
            return false;
        }
        if (c.bends && c.bends.length > 0) {
            return false;
        }

        const srcId = c.sourceGroupId !== currentProcessGroupId ? c.sourceGroupId : c.sourceId;
        const dstId = c.destinationGroupId !== currentProcessGroupId ? c.destinationGroupId : c.destinationId;
        const otherKey = [srcId, dstId].sort().join('::');

        return otherKey === pairKey;
    });
}
