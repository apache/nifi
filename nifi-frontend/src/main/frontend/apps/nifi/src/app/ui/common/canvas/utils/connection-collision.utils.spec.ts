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

import * as d3 from 'd3';
import { ComponentType } from '@nifi/shared';
import { CanvasConnection, CanvasRootSelection } from '../canvas.types';
import { ConnectableComponentDatum } from '../connectable-behavior.helper';
import {
    calculateConnectionCollisionBends,
    calculateConnectionCollisionBendsByIds
} from './connection-collision.utils';

interface SetupOptions {
    sourceId?: string;
    destinationId?: string;
    connectionId?: string;
    sourceGroupId?: string;
    destinationGroupId?: string;
    bends?: Array<{ x: number; y: number }>;
}

function setup(options: SetupOptions = {}) {
    const source = {
        id: options.sourceId ?? 'source',
        position: { x: 0, y: 0 },
        dimensions: { width: 100, height: 100 }
    };
    const destination = {
        id: options.destinationId ?? 'destination',
        position: { x: 400, y: 0 },
        dimensions: { width: 100, height: 100 }
    };
    const connection = {
        id: options.connectionId ?? 'existing',
        sourceId: 'source',
        sourceGroupId: options.sourceGroupId ?? 'root',
        destinationId: 'destination',
        destinationGroupId: options.destinationGroupId ?? 'root',
        bends: options.bends ?? [],
        labelIndex: 0
    };

    return { source, destination, connection };
}

describe('calculateConnectionCollisionBends', () => {
    it('offsets a reconnect when a straight connection already joins the same components', () => {
        const { source, destination, connection } = setup();

        const bends = calculateConnectionCollisionBends(source, destination, [connection], 'root', 'reconnecting');

        expect(bends).toEqual([{ x: 250, y: -25 }]);
    });

    it('does not treat the connection being reconnected as a collision', () => {
        const { source, destination, connection } = setup();

        const bends = calculateConnectionCollisionBends(source, destination, [connection], 'root', connection.id);

        expect(bends).toEqual([]);
    });

    it('uses a child group as the endpoint for a connection attached to its port', () => {
        const { source, destination, connection } = setup({
            sourceId: 'child-group',
            sourceGroupId: 'child-group'
        });

        const bends = calculateConnectionCollisionBends(source, destination, [connection], 'root');

        expect(bends).toEqual([{ x: 250, y: -25 }]);
    });

    it('resolves duplicate component ids within the supplied canvas root', () => {
        document.body.innerHTML = `
            <svg><g id="canvas-one"><g id="id-source"></g><g id="id-destination"></g><g class="connection"></g></g></svg>
            <svg><g id="canvas-two"><g id="id-source"></g><g id="id-destination"></g><g class="connection"></g></g></svg>
        `;
        const firstRoot = d3.select<SVGGElement, unknown>('#canvas-one') as CanvasRootSelection;
        const secondRoot = d3.select<SVGGElement, unknown>('#canvas-two') as CanvasRootSelection;
        bindComponent(firstRoot, 'source', 0);
        bindComponent(firstRoot, 'destination', 400);
        bindConnection(firstRoot);
        bindComponent(secondRoot, 'source', 1000);
        bindComponent(secondRoot, 'destination', 1400);
        bindConnection(secondRoot);

        const bends = calculateConnectionCollisionBendsByIds(
            secondRoot,
            'source',
            'destination',
            'root',
            'reconnecting'
        );

        expect(bends).toEqual([{ x: 1250, y: -25 }]);
        document.body.innerHTML = '';
    });
});

function bindComponent(root: CanvasRootSelection, id: string, x: number): void {
    root.select(`#id-${id}`).datum({
        entity: { id, position: { x, y: 0 } },
        ui: { componentType: ComponentType.Processor, dimensions: { width: 100, height: 100 } }
    } as unknown as ConnectableComponentDatum);
}

function bindConnection(root: CanvasRootSelection): void {
    root.select('g.connection').datum({
        entity: {
            id: 'existing',
            sourceId: 'source',
            sourceGroupId: 'root',
            destinationId: 'destination',
            destinationGroupId: 'root',
            bends: [],
            labelIndex: 0
        },
        ui: { componentType: ComponentType.Connection }
    } as unknown as CanvasConnection);
}
