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

import { TextEllipsisUtils } from '../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../canvas-format-utils.service';
import { CanvasComponentUtils } from '../canvas-component-utils.service';
import { NiFiCommon, Position } from '@nifi/shared';
import { DocumentedType, RegistryClientEntity } from '../../../../state/shared';
import {
    CanvasLabel,
    CanvasProcessor,
    CanvasFunnel,
    CanvasPort,
    CanvasRemoteProcessGroup,
    CanvasProcessGroup,
    CanvasConnection,
    CanvasRootResolver,
    CanvasRootSelection
} from '../canvas.types';
import { ConnectableComponentSelection } from '../connectable-behavior.helper';
import { ConnectionEndpointReconnectDestination } from '../../../../state/flow-shared';

export type ComponentRenderCallbacks<T> = {
    onClick?: (component: T, event: MouseEvent) => void;
    onDoubleClick?: (component: T, event: MouseEvent) => void;
};

export type ComponentDragEndCallback<T> = (component: T, newPosition: Position, previousPosition: Position) => void;

/**
 * Shared callback contract for the typed component-drag gesture seam.
 *
 * The moving ID snapshot is captured when the gesture starts so consumers do
 * not need to re-read selection state after the drag has completed.
 */
export type LayerDragEndCallback = (delta: Position, movingIds: Set<string>) => void;

export interface BaseRenderContext {
    containerSelection: CanvasRootSelection;
    textEllipsis: TextEllipsisUtils;
    formatUtils: CanvasFormatUtils;
    nifiCommon: NiFiCommon;
    getCanEdit: () => boolean;
    getCanSelect: () => boolean;
    getSelectedIds: () => Set<string>;
    canvasRootResolver: CanvasRootResolver;
}

export interface LabelRenderContext extends BaseRenderContext {
    scale: number;
    labels: CanvasLabel[];
    disabledLabelIds?: Set<string>;
    getDisabledLabelIds: () => Set<string>;
    canSelect: boolean;
    callbacks: ComponentRenderCallbacks<CanvasLabel> & {
        onResizeEnd?: (label: CanvasLabel, dimensions: { width: number; height: number }) => void;
        onDragEnd?: LayerDragEndCallback;
    };
}

export interface ProcessorRenderContext extends BaseRenderContext {
    componentUtils: CanvasComponentUtils;
    processors: CanvasProcessor[];
    previewExtensions: DocumentedType[];
    disabledProcessorIds?: Set<string>;
    getDisabledProcessorIds: () => Set<string>;
    canSelect: boolean;
    callbacks: ComponentRenderCallbacks<CanvasProcessor> & {
        onDragEnd?: LayerDragEndCallback;
    };
}

export interface FunnelRenderContext extends BaseRenderContext {
    containerSelection: CanvasRootSelection;
    textEllipsis: TextEllipsisUtils;
    formatUtils: CanvasFormatUtils;
    funnels: CanvasFunnel[];
    canSelect: boolean;
    disabledFunnelIds?: Set<string>;
    getDisabledFunnelIds: () => Set<string>;
    callbacks: ComponentRenderCallbacks<CanvasFunnel> & {
        onDragEnd?: LayerDragEndCallback;
    };
}

export interface PortRenderContext extends BaseRenderContext {
    containerSelection: CanvasRootSelection;
    textEllipsis: TextEllipsisUtils;
    formatUtils: CanvasFormatUtils;
    componentUtils: CanvasComponentUtils;
    ports: CanvasPort[];
    disabledPortIds?: Set<string>;
    canSelect: boolean;
    callbacks: ComponentRenderCallbacks<CanvasPort> & {
        onDragEnd?: LayerDragEndCallback;
    };
    getDisabledPortIds: () => Set<string>;
}

export interface RemoteProcessGroupRenderContext extends BaseRenderContext {
    componentUtils: CanvasComponentUtils;
    remoteProcessGroups: CanvasRemoteProcessGroup[];
    disabledRemoteProcessGroupIds?: Set<string>;
    getDisabledRemoteProcessGroupIds: () => Set<string>;
    canSelect: boolean;
    callbacks: ComponentRenderCallbacks<CanvasRemoteProcessGroup> & {
        onDragEnd?: LayerDragEndCallback;
    };
}

export interface ProcessGroupRenderContext extends BaseRenderContext {
    componentUtils: CanvasComponentUtils;
    processGroups: CanvasProcessGroup[];
    disabledProcessGroupIds?: Set<string>;
    getDisabledProcessGroupIds: () => Set<string>;
    getIsDropAllowed: () => boolean;
    registryClients: RegistryClientEntity[];
    canSelect: boolean;
    callbacks: ComponentRenderCallbacks<CanvasProcessGroup> & {
        onDragEnd?: LayerDragEndCallback;
    };
}

export interface ConnectionReconnectContext {
    isValidConnectionDestination: (selection: ConnectableComponentSelection) => boolean;
    getPerimeterPoint: (point: Position, bounds: { x: number; y: number; width: number; height: number }) => Position;
    selfLoopXOffset: number;
    selfLoopYOffset: number;
}

export interface ConnectionRenderContext extends BaseRenderContext {
    connections: CanvasConnection[];
    processGroupId: string | null;
    canSelect: boolean;
    disabledConnectionIds?: Set<string>;
    getDisabledConnectionIds: () => Set<string>;
    componentUtils: CanvasComponentUtils;
    reconnect?: ConnectionReconnectContext;
    getReconnect: () => ConnectionReconnectContext | undefined;
    callbacks: ComponentRenderCallbacks<CanvasConnection> & {
        onBendPointDragEnd?: (connection: CanvasConnection, bends: Array<{ x: number; y: number }>) => void;
        onBendPointAdd?: (connection: CanvasConnection, point: { x: number; y: number; index: number }) => void;
        onBendPointRemove?: (connection: CanvasConnection, index: number) => void;
        onLabelDragEnd?: (connection: CanvasConnection, labelIndex: number) => void;
        onEndpointReconnect?: (
            connection: CanvasConnection,
            destination: ConnectionEndpointReconnectDestination,
            bends?: Position[]
        ) => void;
    };
}
