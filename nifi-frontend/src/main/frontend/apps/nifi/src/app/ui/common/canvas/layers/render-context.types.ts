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
import { TextEllipsisUtils } from '../utils/text-ellipsis.utils';
import { CanvasFormatUtils } from '../canvas-format-utils.service';
import { CanvasComponentUtils } from '../canvas-component-utils.service';
import { NiFiCommon } from '@nifi/shared';
import { DocumentedType, RegistryClientEntity } from '../../../../state/shared';
import {
    CanvasLabel,
    CanvasProcessor,
    CanvasFunnel,
    CanvasPort,
    CanvasRemoteProcessGroup,
    CanvasProcessGroup,
    CanvasConnection,
    Position
} from '../canvas.types';

export interface BaseRenderContext {
    containerSelection: d3.Selection<any, any, any, any>;
    textEllipsis: TextEllipsisUtils;
    formatUtils: CanvasFormatUtils;
    nifiCommon: NiFiCommon;
    getCanEdit: () => boolean;
}

export interface LabelRenderContext extends BaseRenderContext {
    scale: number;
    labels: CanvasLabel[];
    disabledLabelIds?: Set<string>;
    canSelect: boolean;
    callbacks: {
        onClick?: (label: CanvasLabel, event: MouseEvent) => void;
        onDoubleClick?: (label: CanvasLabel, event: MouseEvent) => void;
        onResizeEnd?: (label: CanvasLabel, dimensions: { width: number; height: number }) => void;
        onDragEnd?: (label: CanvasLabel, newPosition: Position, previousPosition: Position) => void;
    };
}

export interface ProcessorRenderContext extends BaseRenderContext {
    componentUtils: CanvasComponentUtils;
    processors: CanvasProcessor[];
    previewExtensions: DocumentedType[];
    disabledProcessorIds?: Set<string>;
    canSelect: boolean;
    callbacks: {
        onClick?: (processor: CanvasProcessor, event: MouseEvent) => void;
        onDoubleClick?: (processor: CanvasProcessor, event: MouseEvent) => void;
        onDragEnd?: (processor: CanvasProcessor, newPosition: Position, previousPosition: Position) => void;
    };
}

export interface FunnelRenderContext {
    containerSelection: d3.Selection<any, any, any, any>;
    textEllipsis: TextEllipsisUtils;
    formatUtils: CanvasFormatUtils;
    funnels: CanvasFunnel[];
    canSelect: boolean;
    getCanEdit: () => boolean;
    disabledFunnelIds?: Set<string>;
    callbacks: {
        onClick?: (funnel: CanvasFunnel, event: MouseEvent) => void;
        onDoubleClick?: (funnel: CanvasFunnel, event: MouseEvent) => void;
        onDragEnd?: (funnel: CanvasFunnel, newPosition: Position, previousPosition: Position) => void;
    };
}

export interface PortRenderContext {
    containerSelection: d3.Selection<any, any, any, any>;
    textEllipsis: TextEllipsisUtils;
    formatUtils: CanvasFormatUtils;
    componentUtils: CanvasComponentUtils;
    ports: CanvasPort[];
    disabledPortIds?: Set<string>;
    canSelect: boolean;
    getCanEdit: () => boolean;
    callbacks: {
        onClick?: (port: CanvasPort, event: MouseEvent) => void;
        onDoubleClick?: (port: CanvasPort, event: MouseEvent) => void;
        onDragEnd?: (port: CanvasPort, newPosition: Position, previousPosition: Position) => void;
    };
}

export interface RemoteProcessGroupRenderContext extends BaseRenderContext {
    componentUtils: CanvasComponentUtils;
    remoteProcessGroups: CanvasRemoteProcessGroup[];
    disabledRemoteProcessGroupIds?: Set<string>;
    canSelect: boolean;
    callbacks: {
        onClick?: (remoteProcessGroup: CanvasRemoteProcessGroup, event: MouseEvent) => void;
        onDoubleClick?: (remoteProcessGroup: CanvasRemoteProcessGroup, event: MouseEvent) => void;
        onDragEnd?: (
            remoteProcessGroup: CanvasRemoteProcessGroup,
            newPosition: Position,
            previousPosition: Position
        ) => void;
    };
}

export interface ProcessGroupRenderContext extends BaseRenderContext {
    componentUtils: CanvasComponentUtils;
    processGroups: CanvasProcessGroup[];
    disabledProcessGroupIds?: Set<string>;
    registryClients: RegistryClientEntity[];
    canSelect: boolean;
    callbacks: {
        onClick?: (processGroup: CanvasProcessGroup, event: MouseEvent) => void;
        onDoubleClick?: (processGroup: CanvasProcessGroup, event: MouseEvent) => void;
        onDragEnd?: (processGroup: CanvasProcessGroup, newPosition: Position, previousPosition: Position) => void;
    };
}

export interface ConnectionRenderContext extends BaseRenderContext {
    connections: CanvasConnection[];
    processGroupId: string | null;
    canSelect: boolean;
    disabledConnectionIds?: Set<string>;
    componentUtils: CanvasComponentUtils;
    callbacks: {
        onClick?: (connection: CanvasConnection, event: MouseEvent) => void;
        onDoubleClick?: (connection: CanvasConnection, event: MouseEvent) => void;
        onBendPointDragEnd?: (connection: CanvasConnection, bends: Array<{ x: number; y: number }>) => void;
        onBendPointAdd?: (connection: CanvasConnection, point: { x: number; y: number; index: number }) => void;
        onBendPointRemove?: (connection: CanvasConnection, index: number) => void;
        onLabelDragEnd?: (connection: CanvasConnection, labelIndex: number) => void;
    };
}
