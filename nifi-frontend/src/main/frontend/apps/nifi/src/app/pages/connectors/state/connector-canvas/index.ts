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
    Permissions,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    Revision
} from '@nifi/shared';
import { BreadcrumbEntity, ParameterContextEntity, RegistryClientEntity } from '../../../../state/shared';
import {
    CreateComponentRequest,
    CreateLabelRequest,
    CreatePortRequest,
    CreateProcessGroupRequest,
    CreateProcessorRequest,
    CreateRemoteProcessGroupRequest
} from '../../../../state/flow-shared';

export const connectorCanvasFeatureKey = 'connectorCanvas';

export type ConnectorCanvasComponentEntity =
    | ProcessorEntity
    | PortEntity
    | ProcessGroupEntity
    | RemoteProcessGroupEntity
    | (FunnelEntity & {
          dimensions?: never;
          zIndex?: never;
      })
    | ConnectionEntity
    | LabelEntity;

/**
 * Create requests accepted by the connector canvas, discriminated by component type.
 */
export type ConnectorCanvasCreateRequest =
    | (CreateProcessorRequest & { type: ComponentType.Processor })
    | (CreatePortRequest & { type: ComponentType.InputPort | ComponentType.OutputPort })
    | (CreateProcessGroupRequest & { type: ComponentType.ProcessGroup })
    | (CreateRemoteProcessGroupRequest & { type: ComponentType.RemoteProcessGroup })
    | (CreateComponentRequest & { type: ComponentType.Funnel })
    | (CreateLabelRequest & { type: ComponentType.Label });

/**
 * A component created on the connector canvas, discriminated by component type.
 */
export type ConnectorCanvasCreatedComponent =
    | { type: ComponentType.Processor; payload: ProcessorEntity }
    | { type: ComponentType.InputPort | ComponentType.OutputPort; payload: PortEntity }
    | { type: ComponentType.ProcessGroup; payload: ProcessGroupEntity }
    | { type: ComponentType.RemoteProcessGroup; payload: RemoteProcessGroupEntity }
    | { type: ComponentType.Funnel; payload: FunnelEntity }
    | { type: ComponentType.Label; payload: LabelEntity };

/**
 * A component selected for deletion from the connector canvas.
 */
export interface ConnectorCanvasDeleteRequest {
    id: string;
    type: ComponentType;
    revision: Revision;
}

export interface ConnectorCanvasState {
    connectorId: string;
    processGroupId: string | null;
    processGroupRevision: Revision | null;
    parentProcessGroupId: string | null;
    breadcrumb: BreadcrumbEntity | null;
    /**
     * Flow-level permissions for the currently loaded process group, from
     * `ProcessGroupFlowEntity.permissions`. Null until the first flow load completes.
     */
    permissions: Permissions | null;
    labels: LabelEntity[];
    funnels: FunnelEntity[];
    inputPorts: PortEntity[];
    outputPorts: PortEntity[];
    remoteProcessGroups: RemoteProcessGroupEntity[];
    processGroups: ProcessGroupEntity[];
    processors: ProcessorEntity[];
    connections: ConnectionEntity[];
    addedCache: string[];
    removedCache: string[];
    pendingAuthoritativeSnapshot: boolean;
    registryClients: RegistryClientEntity[];
    skipTransform: boolean;
    hasLoadedFlow: boolean;
    loadingStatus: 'pending' | 'loading' | 'success' | 'refresh-error' | 'error';
    error: string | null;
    parameterContext: ParameterContextEntity | null;
    saving: boolean;
    /** True while the user is dragging a palette chip toward the canvas. */
    dragging: boolean;
}

export const initialConnectorCanvasState: ConnectorCanvasState = {
    connectorId: '',
    processGroupId: null,
    processGroupRevision: null,
    parentProcessGroupId: null,
    breadcrumb: null,
    permissions: null,
    labels: [],
    funnels: [],
    inputPorts: [],
    outputPorts: [],
    remoteProcessGroups: [],
    processGroups: [],
    processors: [],
    connections: [],
    addedCache: [],
    removedCache: [],
    pendingAuthoritativeSnapshot: false,
    registryClients: [],
    skipTransform: false,
    hasLoadedFlow: false,
    loadingStatus: 'pending',
    error: null,
    parameterContext: null,
    saving: false,
    dragging: false
};
