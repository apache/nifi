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
    ComponentType,
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity
} from '@nifi/shared';

interface SelectionUiBase<T extends ComponentType> {
    componentType: T;
}

interface PositionableSelectionUi<T extends ComponentType> extends SelectionUiBase<T> {
    dimensions?: { width: number; height: number };
}

interface ProcessorSelectionUi extends PositionableSelectionUi<ComponentType.Processor> {
    preview?: boolean;
}

export interface ProcessorSelectionTarget {
    ui: ProcessorSelectionUi;
    entity: ProcessorEntity;
}

export interface ConnectionSelectionTarget {
    ui: SelectionUiBase<ComponentType.Connection>;
    entity: ConnectionEntity;
}

export interface PortSelectionTarget {
    ui: PositionableSelectionUi<ComponentType.InputPort | ComponentType.OutputPort>;
    entity: PortEntity;
}

export interface ProcessGroupSelectionTarget {
    ui: PositionableSelectionUi<ComponentType.ProcessGroup>;
    entity: ProcessGroupEntity;
}

export interface RemoteProcessGroupSelectionTarget {
    ui: PositionableSelectionUi<ComponentType.RemoteProcessGroup>;
    entity: RemoteProcessGroupEntity;
}

export interface FunnelSelectionTarget {
    ui: PositionableSelectionUi<ComponentType.Funnel>;
    entity: FunnelEntity;
}

export interface LabelSelectionTarget {
    ui: PositionableSelectionUi<ComponentType.Label>;
    entity: LabelEntity;
}

export type SelectionTarget =
    | ProcessorSelectionTarget
    | ConnectionSelectionTarget
    | PortSelectionTarget
    | ProcessGroupSelectionTarget
    | RemoteProcessGroupSelectionTarget
    | FunnelSelectionTarget
    | LabelSelectionTarget;

export type NamedComponentSelectionTarget =
    | ProcessorSelectionTarget
    | PortSelectionTarget
    | ProcessGroupSelectionTarget
    | RemoteProcessGroupSelectionTarget;

export function isConnectionTarget(target: SelectionTarget): target is ConnectionSelectionTarget {
    return target.ui.componentType === ComponentType.Connection;
}

export function isProcessorTarget(target: SelectionTarget): target is ProcessorSelectionTarget {
    return target.ui.componentType === ComponentType.Processor;
}

export function isNamedComponentTarget(target: SelectionTarget): target is NamedComponentSelectionTarget {
    switch (target.ui.componentType) {
        case ComponentType.Processor:
        case ComponentType.InputPort:
        case ComponentType.OutputPort:
        case ComponentType.ProcessGroup:
        case ComponentType.RemoteProcessGroup:
            return true;
        default:
            return false;
    }
}
