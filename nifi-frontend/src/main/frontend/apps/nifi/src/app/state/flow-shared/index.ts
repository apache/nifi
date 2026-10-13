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
    Bundle,
    ComponentType,
    ConnectableDTO,
    ConnectionDTO,
    ConnectionEntity,
    CreateRevisionRequest,
    FunnelDTO,
    FunnelEntity,
    LabelDTO,
    LabelEntity,
    Permissions,
    PortDTO,
    PortEntity,
    Position,
    PositionableComponentEntityBase,
    ProcessGroupDTO,
    ProcessGroupEntity,
    ProcessorDTO,
    ProcessorEntity,
    RemoteProcessGroupDTO,
    RemoteProcessGroupEntity,
    Revision,
    RevisionRequest,
    VersionControlInformationDTO
} from '@nifi/shared';
import { CopyResponseEntity, PasteRequestStrategy } from '../copy';
import { ParameterContextEntity, RegistryClientEntity, SparseVersionedFlow } from '../shared';
import { VersionControlInformation } from '../../ui/common/tooltips/version-control-tip/version-control-tip.component';

/**
 * Lightweight reference to a canvas component.
 */
export interface CanvasComponentRef {
    id: string;
    type: ComponentType;
}

/**
 * Canonical entity kinds that can participate in a connection gesture.
 * Connections and labels cannot be connection sources or destinations.
 */
export type CanvasConnectableEntity =
    | ProcessorEntity
    | PortEntity
    | ProcessGroupEntity
    | RemoteProcessGroupEntity
    | (FunnelEntity & {
          dimensions?: never;
          zIndex?: never;
      });

/**
 * Union of every canonical entity kind rendered on the reusable canvas.
 */
export type CanvasComponentEntity = CanvasConnectableEntity | ConnectionEntity | LabelEntity;

/**
 * Destination selected by an endpoint reconnect gesture.
 */
export interface ConnectionEndpointReconnectDestination {
    id: string;
    componentType: ComponentType;
    entity: CanvasConnectableEntity;
}

/**
 * Common mutable-entity request envelope used by NiFi component endpoints.
 */
export interface ComponentWriteRequest<TComponent> {
    revision: RevisionRequest;
    disconnectedNodeAcknowledged?: boolean;
    component: TComponent;
}

/**
 * Common mutable-entity update envelope. Update component bodies identify the
 * component independently of the resource identifier in the request path.
 */
export type ComponentUpdateRequest<TComponent extends { id: string }> = ComponentWriteRequest<TComponent>;

export interface ComponentCreateRequest<TComponent> extends Omit<ComponentWriteRequest<TComponent>, 'revision'> {
    revision: CreateRevisionRequest;
}

type RequiredPosition<TComponent extends { position?: PositionableComponentEntityBase<unknown>['position'] }> =
    Required<Pick<TComponent, 'position'>>;

export type ProcessorCreateBody = RequiredPosition<ProcessorDTO> & Required<Pick<ProcessorDTO, 'type' | 'bundle'>>;
export type ProcessorCreateRequest = ComponentCreateRequest<ProcessorCreateBody>;

export type PortCreateBody = RequiredPosition<PortDTO> & Required<Pick<PortDTO, 'name' | 'allowRemoteAccess'>>;
export type PortCreateRequest = ComponentCreateRequest<PortCreateBody>;

export type ProcessGroupCreateBody = RequiredPosition<ProcessGroupDTO> &
    Required<Pick<ProcessGroupDTO, 'name'>> & {
        parameterContext?: {
            id: NonNullable<ProcessGroupDTO['parameterContext']>['id'];
        };
    };
export type ProcessGroupCreateRequest = ComponentCreateRequest<ProcessGroupCreateBody>;

export type ProcessGroupImportBody = RequiredPosition<ProcessGroupDTO> & {
    versionControlInformation: Pick<VersionControlInformationDTO, 'registryId' | 'bucketId' | 'flowId' | 'version'> &
        Partial<Pick<VersionControlInformationDTO, 'branch'>>;
};
export type ProcessGroupImportRequest = ComponentCreateRequest<ProcessGroupImportBody>;

type RemoteProcessGroupCreateOptions = Pick<
    RemoteProcessGroupDTO,
    | 'transportProtocol'
    | 'localNetworkInterface'
    | 'proxyHost'
    | 'proxyPort'
    | 'proxyUser'
    | 'proxyPassword'
    | 'communicationsTimeout'
    | 'yieldDuration'
>;

export type RemoteProcessGroupCreateBody = RequiredPosition<RemoteProcessGroupDTO> &
    Required<Pick<RemoteProcessGroupDTO, 'targetUris'>> &
    Partial<RemoteProcessGroupCreateOptions>;
export type RemoteProcessGroupCreateRequest = ComponentCreateRequest<RemoteProcessGroupCreateBody>;

export type FunnelCreateBody = RequiredPosition<FunnelDTO>;
export type FunnelCreateRequest = ComponentCreateRequest<FunnelCreateBody>;

export type LabelCreateBody = RequiredPosition<LabelDTO> &
    Required<Pick<LabelDTO, 'zIndex'>> &
    Partial<Pick<LabelDTO, 'label' | 'style' | 'width' | 'height'>>;
export type LabelCreateRequest = ComponentCreateRequest<LabelCreateBody>;

export type ConnectionEndpointReference = Pick<ConnectableDTO, 'id' | 'groupId' | 'type'>;

type ConnectionCreateOptions = Pick<
    ConnectionDTO,
    | 'name'
    | 'selectedRelationships'
    | 'backPressureObjectThreshold'
    | 'backPressureDataSizeThreshold'
    | 'flowFileExpiration'
    | 'prioritizers'
    | 'loadBalanceStrategy'
    | 'loadBalanceCompression'
    | 'loadBalancePartitionAttribute'
    | 'zIndex'
>;

export type ConnectionCreateBody = Required<Pick<ConnectionDTO, 'labelIndex'>> &
    Partial<Pick<ConnectionDTO, 'bends'>> &
    Partial<ConnectionCreateOptions> & {
        source: ConnectionEndpointReference;
        destination: ConnectionEndpointReference;
    };
export type ConnectionCreateRequest = ComponentCreateRequest<ConnectionCreateBody>;

export type MovableComponentType =
    | ComponentType.Processor
    | ComponentType.InputPort
    | ComponentType.OutputPort
    | ComponentType.ProcessGroup
    | ComponentType.RemoteProcessGroup
    | ComponentType.Funnel
    | ComponentType.Label;

export type MovableComponentUpdate = Pick<PositionableComponentEntityBase<unknown>, 'id' | 'position'> & {
    type: MovableComponentType;
    revision: RevisionRequest;
};

export type ConnectionGeometryUpdate = Pick<ConnectionEntity, 'id' | 'labelIndex'> &
    Required<Pick<ConnectionEntity, 'bends'>> & {
        revision: RevisionRequest;
    };

export type ConnectionDestinationUpdate = Pick<ConnectionEntity, 'id'> & {
    revision: RevisionRequest;
    destination: ConnectionEndpointReference;
};

export type ComponentRunStatusRequest = Pick<ProcessorDTO, 'state'> & {
    revision: RevisionRequest;
    disconnectedNodeAcknowledged?: boolean;
};

export type SnippetRevisionMap = Record<string, RevisionRequest>;

export interface SnippetRevisionMaps {
    processors: SnippetRevisionMap;
    inputPorts: SnippetRevisionMap;
    outputPorts: SnippetRevisionMap;
    processGroups: SnippetRevisionMap;
    remoteProcessGroups: SnippetRevisionMap;
    funnels: SnippetRevisionMap;
    labels: SnippetRevisionMap;
    connections: SnippetRevisionMap;
}

export interface SnippetCreateBody extends SnippetRevisionMaps {
    parentGroupId: string;
}

export interface SnippetCreateRequest {
    disconnectedNodeAcknowledged?: boolean;
    snippet: SnippetCreateBody;
}

export interface SnippetMoveBody {
    id: string;
    parentGroupId: string;
}

export interface SnippetMoveRequest {
    disconnectedNodeAcknowledged?: boolean;
    snippet: SnippetMoveBody;
}

export interface SnippetDeleteQuery {
    disconnectedNodeAcknowledged: boolean;
}

export interface SnippetDeleteOptions {
    params: SnippetDeleteQuery;
}

export interface SnippetDeleteRequest {
    id: string;
    options: SnippetDeleteOptions;
}

/*
 * Requests shared by the flow designer and the reusable component dialogs.
 */

export interface SelectedComponent {
    id: string;
    componentType: ComponentType;
    entity?: any;
}

export interface CreateComponentRequest {
    type: ComponentType;
    position: Position;
    revision: CreateRevisionRequest;
}

export interface CreateLabelRequest extends CreateComponentRequest {
    zIndex: number;
}

export interface CreateConnectionRequest {
    source: SelectedComponent;
    destination: SelectedComponent;
    bends?: Position[];
}

export interface CreateConnectionDialogRequest {
    request: CreateConnectionRequest;
    defaults: {
        flowfileExpiration: string;
        objectThreshold: number;
        dataSizeThreshold: string;
    };
}

export interface CreateConnection {
    payload: ConnectionCreateRequest;
}

export interface CreateProcessGroupDialogRequest {
    request: CreateComponentRequest;
    currentParameterContextId?: string;
    parameterContexts: ParameterContextEntity[];
}

export interface NoRegistryClientsDialogRequest {
    controllerPermissions: Permissions;
}

export interface ImportFromRegistryDialogRequest {
    request: CreateComponentRequest;
    registryClients: RegistryClientEntity[];
}

export interface ImportFromRegistryRequest {
    payload: ProcessGroupImportRequest;
    keepExistingParameterContext: boolean;
}

export interface SaveToVersionControlRequest {
    processGroupId: string;
    versionedFlow: SparseVersionedFlow;
    processGroupRevision: Revision;
}

export interface StopVersionControlRequest {
    revision: Revision;
    processGroupId: string;
}

export interface VersionControlInformationEntity {
    processGroupRevision: Revision;
    versionControlInformation?: VersionControlInformation;
    disconnectedNodeAcknowledged?: boolean;
}

export interface OpenGroupComponentsDialogRequest {
    position: Position;
    moveComponents: MoveComponentRequest[];
}

export interface GroupComponentsDialogRequest {
    request: OpenGroupComponentsDialogRequest;
    currentParameterContextId?: string;
    parameterContexts: ParameterContextEntity[];
}

export interface GroupComponentsRequest extends CreateProcessGroupRequest {
    components: MoveComponentRequest[];
}

export interface GroupComponentsSuccess extends CreateComponentResponse {
    components: MoveComponentRequest[];
}

export interface CreateProcessorDialogRequest {
    request: CreateComponentRequest;
}

export interface GoToRemoteProcessGroupRequest {
    uri: string;
}

export interface CreateProcessorRequest extends CreateComponentRequest {
    processorType: string;
    processorBundle: Bundle;
}

export interface CreateProcessGroupRequest extends CreateComponentRequest {
    name: string;
    parameterContextId: string | null;
}

export interface UploadProcessGroupRequest extends CreateComponentRequest {
    name: string;
    flowDefinition: File;
}

export interface CreateRemoteProcessGroupRequest extends CreateComponentRequest {
    targetUris: string;
    transportProtocol: string;
    localNetworkInterface: string;
    proxyHost: string;
    proxyPort: string;
    proxyUser: string;
    proxyPassword: string;
    communicationsTimeout: string;
    yieldDuration: string;
}

export interface CreatePortRequest extends CreateComponentRequest {
    name: string;
    allowRemoteAccess: boolean;
}

export interface CreateComponentResponse {
    type: ComponentType;
    payload: any;
}

export interface SnippetComponentRequest {
    id: string;
    uri: string;
    type: ComponentType;
    entity: CanvasComponentEntity;
}

export type MoveComponentRequest = SnippetComponentRequest;

export interface PasteRequest {
    copyResponse: CopyResponseEntity;
    strategy: PasteRequestStrategy;
    fitToScreen?: boolean;
    bbox?: any;
}

export interface PasteRequestEntity {
    copyResponse: CopyResponseEntity;
    revision: Revision;
    disconnectedNodeAcknowledged?: boolean;
}

export interface PasteRequestContext {
    processGroupId: string;
    pasteRequest: PasteRequestEntity;
    pasteStrategy: PasteRequestStrategy;
}

export interface DeleteComponentRequest {
    id: string;
    uri: string;
    type: ComponentType;
    entity: any;
}

export interface ReplayLastProvenanceEventRequest {
    componentId: string;
    nodes: string;
}

export interface Snippet {
    parentGroupId: string;
    processors: {
        [key: string]: any;
    };
    funnels: {
        [key: string]: any;
    };
    inputPorts: {
        [key: string]: any;
    };
    outputPorts: {
        [key: string]: any;
    };
    remoteProcessGroups: {
        [key: string]: any;
    };
    processGroups: {
        [key: string]: any;
    };
    connections: {
        [key: string]: any;
    };
    labels: {
        [key: string]: any;
    };
}

export interface Dimensions {
    width: number;
    height: number;
}

export interface RunOnceRequest {
    id: string;
    revision: Revision;
}

export interface EnableProcessGroupRequest {
    id: string;
    type: ComponentType;
    errorStrategy: 'snackbar' | 'banner';
}

export interface DisableProcessGroupRequest {
    id: string;
    type: ComponentType;
    errorStrategy: 'snackbar' | 'banner';
}

export interface StartProcessGroupRequest {
    id: string;
    type: ComponentType;
    errorStrategy: 'snackbar' | 'banner';
}

export interface ProcessGroupRunStatusRequest {
    id: string;
    state: string;
    disconnectedNodeAcknowledged: boolean;
}

export interface StopProcessGroupRequest {
    id: string;
    type: ComponentType;
    errorStrategy: 'snackbar' | 'banner';
}

export interface ControllerServiceStateRequest {
    id: string;
    state: string;
    disconnectedNodeAcknowledged: boolean;
}

export interface TerminateThreadsRequest {
    id: string;
    uri: string;
}

export interface ClearBulletinsForGroupRequest {
    processGroupId: string;
    fromTimestamp: string;
}

export interface FlowUpdateRequest {
    requestId: string;
    processGroupId: string;
    uri: string;
    lastUpdated: string;
    complete: boolean;
    percentCompleted: number;
    state: string;
    failureReason?: string;
}

export interface FlowUpdateRequestEntity {
    processGroupRevision: Revision;
    request: FlowUpdateRequest;
}

export interface Difference {
    differenceType: string;
    difference: string;
    environmental?: boolean;
}

export interface ComponentDifference {
    componentType: ComponentType;
    componentId: string;
    processGroupId: string;
    differences: Difference[];
    componentName?: string;
}

export interface FlowComparisonEntity {
    componentDifferences: ComponentDifference[];
}

export interface DownloadFlowRequest {
    processGroupId: string;
    includeReferencedServices: boolean;
}
