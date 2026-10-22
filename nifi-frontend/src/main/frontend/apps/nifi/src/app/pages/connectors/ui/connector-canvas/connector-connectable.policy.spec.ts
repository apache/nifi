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
import { ConnectorConnectablePolicy } from './connector-connectable.policy';
import {
    ConnectableComponentDatum,
    ConnectableComponentSelection
} from '../../../../ui/common/canvas/connectable-behavior.helper';
import { makeProcessor, makeProcessorDto } from '../../testing/connector-canvas-entity-fixtures';

function selection(
    type: ComponentType,
    options: { canRead?: boolean; canWrite?: boolean; inputRequirement?: string } = {}
): ConnectableComponentSelection {
    const canRead = options.canRead ?? true;
    const element = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    const datum = {
        entity: makeProcessor('component-1', {
            permissions: {
                canRead,
                canWrite: options.canWrite ?? true
            },
            ...(options.inputRequirement !== undefined ? { inputRequirement: options.inputRequirement } : {}),
            ...(canRead
                ? {
                      component: makeProcessorDto(
                          options.inputRequirement !== undefined ? { inputRequirement: options.inputRequirement } : {}
                      )
                  }
                : {})
        }),
        ui: { componentType: type, dimensions: { width: 100, height: 50 } }
    } as unknown as ConnectableComponentDatum;
    return d3
        .select<SVGGElement, ConnectableComponentDatum>(element)
        .datum(datum) as unknown as ConnectableComponentSelection;
}

describe('ConnectorConnectablePolicy', () => {
    const policy = new ConnectorConnectablePolicy();

    it('applies source type and permission gates', () => {
        expect(policy.isValidConnectionSource(selection(ComponentType.Processor))).toBe(true);
        expect(policy.isValidConnectionSource(selection(ComponentType.InputPort))).toBe(true);
        expect(policy.isValidConnectionSource(selection(ComponentType.OutputPort))).toBe(false);
        expect(policy.isValidConnectionSource(selection(ComponentType.Processor, { canRead: false }))).toBe(false);
        expect(policy.isValidConnectionSource(selection(ComponentType.ProcessGroup, { canWrite: false }))).toBe(true);
    });

    it('applies destination type, permission, and input requirement gates', () => {
        expect(policy.isValidConnectionDestination(selection(ComponentType.OutputPort))).toBe(true);
        expect(policy.isValidConnectionDestination(selection(ComponentType.InputPort))).toBe(false);
        expect(
            policy.isValidConnectionDestination(
                selection(ComponentType.Processor, { inputRequirement: 'INPUT_FORBIDDEN' })
            )
        ).toBe(false);
        expect(policy.isValidConnectionDestination(selection(ComponentType.Processor, { canWrite: false }))).toBe(
            false
        );
        expect(policy.isValidConnectionDestination(selection(ComponentType.ProcessGroup, { canWrite: false }))).toBe(
            true
        );
        expect(
            policy.isValidConnectionDestination(
                selection(ComponentType.Processor, { canRead: false, inputRequirement: 'INPUT_FORBIDDEN' })
            )
        ).toBe(false);
        expect(policy.isValidConnectionDestination(selection(ComponentType.Processor))).toBe(true);
    });

    it('creates self-loop bends for a new connection', () => {
        const datum = selection(ComponentType.Processor).datum();

        const bends = policy.geometry.calculateInitialBendPoints(datum, datum);

        expect(bends).toHaveLength(2);
        expect(bends[0].x).toBeGreaterThan(datum.entity.position.x + datum.ui.dimensions.width);
        expect(bends[0].y).toBeLessThan(bends[1].y);
    });
});
