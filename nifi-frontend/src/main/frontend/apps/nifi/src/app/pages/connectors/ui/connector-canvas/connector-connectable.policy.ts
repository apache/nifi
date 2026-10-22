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
import { CanvasProcessor, CanvasRootResolver } from '../../../../ui/common/canvas/canvas.types';
import {
    ConnectableComponentDatum,
    ConnectableComponentSelection,
    ConnectablePolicy,
    DEFAULT_GEOMETRY,
    resolveConnectablePosition
} from '../../../../ui/common/canvas/connectable-behavior.helper';
import { calculateConnectionCollisionBendsByIds } from '../../../../ui/common/canvas/utils/connection-collision.utils';

export class ConnectorConnectablePolicy implements ConnectablePolicy {
    readonly geometry = {
        calculateInitialBendPoints: (
            sourceData: ConnectableComponentDatum,
            destinationData: ConnectableComponentDatum
        ) => {
            const sourcePosition = resolveConnectablePosition(sourceData);
            if (sourceData.entity.id === destinationData.entity.id) {
                const x = sourcePosition.x + sourceData.ui.dimensions.width + DEFAULT_GEOMETRY.selfLoopXOffset;
                const y = sourcePosition.y + sourceData.ui.dimensions.height / 2;
                return [
                    { x, y: y - DEFAULT_GEOMETRY.selfLoopYOffset },
                    { x, y: y + DEFAULT_GEOMETRY.selfLoopYOffset }
                ];
            }

            const processGroupId = this.processGroupIdResolver?.();
            if (!this.canvasRootResolver || !processGroupId) {
                return [];
            }

            return calculateConnectionCollisionBendsByIds(
                this.canvasRootResolver(),
                sourceData.entity.id,
                destinationData.entity.id,
                processGroupId
            );
        }
    };

    constructor(
        private readonly canvasRootResolver?: CanvasRootResolver,
        private readonly processGroupIdResolver?: () => string | null
    ) {}

    isValidConnectionSource(selection: ConnectableComponentSelection): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        const datum = selection.datum();
        const componentType = datum?.ui.componentType;
        if (componentType === ComponentType.ProcessGroup) {
            return true;
        }
        if (!hasCanRead(datum) || !hasCanWrite(datum)) {
            return false;
        }

        return (
            componentType === ComponentType.Processor ||
            componentType === ComponentType.RemoteProcessGroup ||
            componentType === ComponentType.InputPort ||
            componentType === ComponentType.Funnel
        );
    }

    isValidConnectionDestination(selection: ConnectableComponentSelection): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        const datum = selection.datum();
        const componentType = datum?.ui.componentType;
        if (componentType === ComponentType.ProcessGroup) {
            return true;
        }
        if (!hasCanWrite(datum)) {
            return false;
        }
        if (
            componentType === ComponentType.RemoteProcessGroup ||
            componentType === ComponentType.OutputPort ||
            componentType === ComponentType.Funnel
        ) {
            return true;
        }
        if (componentType === ComponentType.Processor) {
            // EntityFactory publishes inputRequirement on the processor entity
            // before attaching component, so a writable destination that cannot
            // be read still exposes the field.
            return (datum as CanvasProcessor).entity.inputRequirement !== 'INPUT_FORBIDDEN';
        }
        return false;
    }
}

function hasCanRead(datum: ConnectableComponentDatum | undefined): boolean {
    return datum?.entity.permissions.canRead === true;
}

function hasCanWrite(datum: ConnectableComponentDatum | undefined): boolean {
    return datum?.entity.permissions.canWrite === true;
}
