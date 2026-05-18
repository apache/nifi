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

export interface Position {
    x: number;
    y: number;
}

export interface Dimension {
    width: number;
    height: number;
}

export interface LabelUiState {
    componentType: ComponentType.Label;
    dimensions: Dimension;
    dragStartPosition?: Position; // Stored during drag for potential revert on error
    currentPosition?: Position; // Optimistic position during drag (before API confirmation)
}

export interface ProcessorUiState {
    componentType: ComponentType.Processor;
    dimensions: Dimension;
    dragStartPosition?: Position; // Stored during drag for potential revert on error
    currentPosition?: Position; // Current position during drag (overrides entity.position)
}

export interface FunnelUiState {
    componentType: ComponentType.Funnel;
    dimensions: Dimension;
    dragStartPosition?: Position; // Stored during drag for potential revert on error
    currentPosition?: Position; // Optimistic position during drag (before API confirmation)
}

export interface PortUiState {
    componentType: ComponentType.InputPort | ComponentType.OutputPort;
    dimensions: Dimension;
    dragStartPosition?: Position; // Stored during drag for potential revert on error
    currentPosition?: Position; // Optimistic position during drag (before API confirmation)
}

export interface RemoteProcessGroupUiState {
    componentType: ComponentType.RemoteProcessGroup;
    dimensions: Dimension;
    dragStartPosition?: Position; // Stored during drag for potential revert on error
    currentPosition?: Position; // Optimistic position during drag (before API confirmation)
}

export interface ProcessGroupUiState {
    componentType: ComponentType.ProcessGroup;
    dimensions: Dimension;
    dragStartPosition?: Position; // Stored during drag for potential revert on error
    currentPosition?: Position; // Optimistic position during drag (before API confirmation)
}

export interface ConnectionUiState {
    componentType: ComponentType.Connection;
    // Calculated at render time by calculatePath() - always set, used for selection box logic
    start: Position;
    end: Position;
    // Bend points - initialized from entity.bends, used for rendering (allows optimistic updates)
    bends?: Position[];
    // Drag state for bend points and label
    dragging?: boolean;
    // Temporary label index during label drag (before save)
    tempLabelIndex?: number;
}

export interface CanvasLabel {
    entity: any; // TODO: Import LabelEntity type from flow state
    ui: LabelUiState;
}

export interface CanvasProcessor {
    entity: any; // TODO: Import ProcessorEntity type from flow state
    ui: ProcessorUiState;
}

export interface CanvasFunnel {
    entity: any; // TODO: Import FunnelEntity type from flow state
    ui: FunnelUiState;
}

export interface CanvasPort {
    entity: any; // TODO: Import InputPortEntity/OutputPortEntity type from flow state
    ui: PortUiState;
}

export interface CanvasRemoteProcessGroup {
    entity: any; // TODO: Import RemoteProcessGroupEntity type from flow state
    ui: RemoteProcessGroupUiState;
}

export interface CanvasProcessGroup {
    entity: any; // TODO: Import ProcessGroupEntity type from flow state
    ui: ProcessGroupUiState;
}

export interface CanvasConnection {
    entity: any; // TODO: Import ConnectionEntity type from flow state
    ui: ConnectionUiState;
}

export type CanvasComponent =
    | CanvasLabel
    | CanvasProcessor
    | CanvasFunnel
    | CanvasPort
    | CanvasRemoteProcessGroup
    | CanvasProcessGroup
    | CanvasConnection;

export interface ContextMenuContext {
    processGroupId: string | null;
    targetType: 'canvas' | 'component';
    selectedComponents: CanvasComponent[];
    clickedComponent?: CanvasComponent;
    allConnections: CanvasConnection[];
}
