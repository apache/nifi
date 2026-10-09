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

import { ComponentType } from '@nifi/shared';
import { describe, expect, it } from 'vitest';
import { SelectionTarget, isConnectionTarget, isNamedComponentTarget, isProcessorTarget } from './selection.utils';

function target(componentType: ComponentType): SelectionTarget {
    return {
        entity: { id: componentType },
        ui: { componentType }
    } as SelectionTarget;
}

describe('selection guards', () => {
    it('identifies connection targets', () => {
        expect(isConnectionTarget(target(ComponentType.Connection))).toBe(true);
        expect(isConnectionTarget(target(ComponentType.Processor))).toBe(false);
    });

    it('identifies processor targets', () => {
        expect(isProcessorTarget(target(ComponentType.Processor))).toBe(true);
        expect(isProcessorTarget(target(ComponentType.Connection))).toBe(false);
    });

    it.each([
        ComponentType.Processor,
        ComponentType.InputPort,
        ComponentType.OutputPort,
        ComponentType.ProcessGroup,
        ComponentType.RemoteProcessGroup
    ])('identifies %s as a named component target', (componentType) => {
        expect(isNamedComponentTarget(target(componentType))).toBe(true);
    });

    it.each([ComponentType.Connection, ComponentType.Funnel, ComponentType.Label])(
        'does not identify %s as a named component target',
        (componentType) => {
            expect(isNamedComponentTarget(target(componentType))).toBe(false);
        }
    );
});
