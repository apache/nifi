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

import { Position } from '@nifi/shared';
import { CanvasConnection, CanvasRootSelection } from '../canvas.types';
import {
    ConnectableComponentDatum,
    ConnectableComponentSelection,
    resolveConnectablePosition
} from '../connectable-behavior.helper';

const CONNECTION_OFFSET_Y_INCREMENT = 75;
const CONNECTION_OFFSET_X_INCREMENT = 250;
const MAX_ATTEMPTS = 100;

interface CollisionComponent {
    id: string;
    position: Position;
    dimensions: { width: number; height: number };
}

interface CollisionConnection {
    id: string;
    sourceId: string;
    sourceGroupId: string;
    destinationId: string;
    destinationGroupId: string;
    bends: Position[];
    labelIndex: number;
}

export function calculateConnectionCollisionBendsByIds(
    canvasRoot: CanvasRootSelection,
    sourceComponentId: string,
    destinationComponentId: string,
    processGroupId: string,
    connectionIdToExclude?: string
): Position[] {
    const sourceSelection = canvasRoot.select<SVGGElement>(`#id-${sourceComponentId}`) as ConnectableComponentSelection;
    const destinationSelection = canvasRoot.select<SVGGElement>(
        `#id-${destinationComponentId}`
    ) as ConnectableComponentSelection;

    if (sourceSelection.empty() || destinationSelection.empty()) {
        return [];
    }

    const connections = canvasRoot
        .selectAll<SVGGElement, CanvasConnection>('g.connection')
        .data()
        .map(({ entity }) => entity);

    return calculateConnectionCollisionBends(
        toCollisionComponent(sourceSelection.datum()),
        toCollisionComponent(destinationSelection.datum()),
        connections,
        processGroupId,
        connectionIdToExclude
    );
}

export function calculateConnectionCollisionBends(
    source: CollisionComponent,
    destination: CollisionComponent,
    connections: CollisionConnection[],
    processGroupId: string,
    connectionIdToExclude?: string
): Position[] {
    const existingConnections = connections.filter((connection) => {
        if (connection.id === connectionIdToExclude) {
            return false;
        }

        const connectionSourceId = getEndpointComponentId(
            connection.sourceId,
            connection.sourceGroupId,
            processGroupId
        );
        const connectionDestinationId = getEndpointComponentId(
            connection.destinationId,
            connection.destinationGroupId,
            processGroupId
        );

        return (
            (connectionSourceId === source.id && connectionDestinationId === destination.id) ||
            (connectionSourceId === destination.id && connectionDestinationId === source.id)
        );
    });

    if (!existingConnections.some((connection) => connection.bends.length === 0)) {
        return [];
    }

    const sourceMiddle = getMiddle(source);
    const destinationMiddle = getMiddle(destination);
    const slope = (sourceMiddle.y - destinationMiddle.y) / (sourceMiddle.x - destinationMiddle.x);
    const isMoreHorizontal = slope <= 1 && slope >= -1;
    const xCandidate = (sourceMiddle.x + destinationMiddle.x) / 2;
    const yCandidate = (sourceMiddle.y + destinationMiddle.y) / 2;

    let xStep = isMoreHorizontal ? 0 : CONNECTION_OFFSET_X_INCREMENT;
    let yStep = isMoreHorizontal ? CONNECTION_OFFSET_Y_INCREMENT : 0;
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        const precedingCandidate = { x: xCandidate - xStep, y: yCandidate - yStep };
        if (!collidesWith(existingConnections, precedingCandidate)) {
            return [precedingCandidate];
        }

        const followingCandidate = { x: xCandidate + xStep, y: yCandidate + yStep };
        if (!collidesWith(existingConnections, followingCandidate)) {
            return [followingCandidate];
        }

        if (isMoreHorizontal) {
            yStep += CONNECTION_OFFSET_Y_INCREMENT;
        } else {
            xStep += CONNECTION_OFFSET_X_INCREMENT;
        }
    }

    return [{ x: xCandidate - xStep, y: yCandidate - yStep }];
}

function toCollisionComponent(datum: ConnectableComponentDatum): CollisionComponent {
    return {
        id: datum.entity.id,
        position: resolveConnectablePosition(datum),
        dimensions: datum.ui.dimensions
    };
}

function getEndpointComponentId(componentId: string, groupId: string, processGroupId: string): string {
    return groupId !== processGroupId ? groupId : componentId;
}

function getMiddle(component: CollisionComponent): Position {
    return {
        x: component.position.x + component.dimensions.width / 2,
        y: component.position.y + component.dimensions.height / 2
    };
}

function collidesWith(connections: CollisionConnection[], candidate: Position): boolean {
    return connections.some((connection) => {
        if (connection.bends.length === 0) {
            return false;
        }

        const labelIndex = connection.labelIndex < connection.bends.length ? connection.labelIndex : 0;
        const labelBend = connection.bends[labelIndex];
        return (
            labelBend.y - 25 < candidate.y &&
            labelBend.y + 25 > candidate.y &&
            labelBend.x - 100 < candidate.x &&
            labelBend.x + 100 > candidate.x
        );
    });
}
