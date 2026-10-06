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
import {
    ComponentType,
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    Position,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    Revision,
    RevisionRequest
} from '@nifi/shared';

/**
 * Typed selection of the canvas root group.
 */
export type CanvasRootSelection = d3.Selection<SVGGElement, unknown, d3.BaseType, unknown>;

/**
 * Resolves the live canvas root group for an interaction event.
 */
export type CanvasRootResolver = () => CanvasRootSelection;

export interface Dimension {
    width: number;
    height: number;
}

interface DragUiState {
    dragDelta?: Position;
    dragMovingIds?: Set<string>;
    dragStartPosition?: Position;
    currentPosition?: Position;
    dragStartEntity?: CanvasEntity;
}

export interface LabelUiState extends DragUiState {
    componentType: ComponentType.Label;
    dimensions: Dimension;
    dragStartRevision?: Revision;
}

export interface ProcessorUiState extends DragUiState {
    componentType: ComponentType.Processor;
    dimensions: Dimension;
    preview?: boolean;
}

export interface FunnelUiState extends DragUiState {
    componentType: ComponentType.Funnel;
    dimensions: Dimension;
}

export interface PortUiState extends DragUiState {
    componentType: ComponentType.InputPort | ComponentType.OutputPort;
    dimensions: Dimension;
}

export interface RemoteProcessGroupUiState extends DragUiState {
    componentType: ComponentType.RemoteProcessGroup;
    dimensions: Dimension;
}

export interface ProcessGroupUiState extends DragUiState {
    componentType: ComponentType.ProcessGroup;
    dimensions: Dimension;
}

export interface ConnectionUiState {
    componentType: ComponentType.Connection;
    // Calculated at render time by calculatePath() - always set, used for selection box logic
    start: Position;
    end: Position;
    // Bend points - initialized from entity.bends, used for rendering (allows optimistic updates)
    bends?: Position[];
    dragStartBends?: Position[];
    // Drag state for bend points and label
    dragging?: boolean;
    endPointDragging?: boolean;
    reconnectDestinationId?: string;
    // Temporary label index during label drag (before save)
    tempLabelIndex?: number;
    dragStartEntity?: CanvasEntity;
    dragStartRevision?: Revision;
}

export interface CanvasLabel {
    entity: LabelEntity;
    ui: LabelUiState;
}

export interface CanvasProcessor {
    entity: ProcessorEntity;
    ui: ProcessorUiState;
}

export interface CanvasFunnel {
    entity: FunnelEntity;
    ui: FunnelUiState;
}

export interface CanvasPort {
    entity: PortEntity;
    ui: PortUiState;
}

export interface CanvasRemoteProcessGroup {
    entity: RemoteProcessGroupEntity;
    ui: RemoteProcessGroupUiState;
}

export interface CanvasProcessGroup {
    entity: ProcessGroupEntity;
    ui: ProcessGroupUiState;
}

export interface CanvasConnection {
    entity: ConnectionEntity;
    ui: ConnectionUiState;
}

export type CanvasDatum =
    | CanvasLabel
    | CanvasProcessor
    | CanvasFunnel
    | CanvasPort
    | CanvasRemoteProcessGroup
    | CanvasProcessGroup
    | CanvasConnection;

/**
 * Compatibility name retained for existing reusable-canvas consumers.
 */
export type CanvasComponent = CanvasDatum;

export function isProcessorDatum(datum: CanvasDatum): datum is CanvasProcessor {
    return datum.ui.componentType === ComponentType.Processor;
}

export function isConnectionDatum(datum: CanvasDatum): datum is CanvasConnection {
    return datum.ui.componentType === ComponentType.Connection;
}

export type ActiveThreadCountDatum = CanvasProcessor | CanvasPort | CanvasProcessGroup | CanvasRemoteProcessGroup;

export type CanvasSelection<TDatum extends CanvasDatum = CanvasDatum> = d3.Selection<
    SVGGElement,
    TDatum,
    d3.BaseType,
    unknown
>;

export type CanvasEntity =
    | LabelEntity
    | ProcessorEntity
    | FunnelEntity
    | PortEntity
    | RemoteProcessGroupEntity
    | ProcessGroupEntity
    | ConnectionEntity;

export type ComponentDoubleClickEvent =
    | { entity: ProcessorEntity; componentType: ComponentType.Processor }
    | { entity: PortEntity; componentType: ComponentType.InputPort | ComponentType.OutputPort }
    | { entity: RemoteProcessGroupEntity; componentType: ComponentType.RemoteProcessGroup }
    | { entity: ConnectionEntity; componentType: ComponentType.Connection };

export type DragEndBaselineItem =
    | {
          id: string;
          type:
              | ComponentType.Processor
              | ComponentType.Funnel
              | ComponentType.InputPort
              | ComponentType.OutputPort
              | ComponentType.ProcessGroup
              | ComponentType.RemoteProcessGroup
              | ComponentType.Label;
          position: Position;
          revision: RevisionRequest;
      }
    | {
          id: string;
          type: ComponentType.Connection;
          bends: Position[];
          revision: RevisionRequest;
          labelIndex?: number;
      };

export interface ContextMenuContext {
    processGroupId: string | null;
    targetType: 'canvas' | 'component';
    selectedComponents: CanvasDatum[];
    clickedComponent?: CanvasDatum;
    allConnections: CanvasConnection[];
}
