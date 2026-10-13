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

import { Position } from '@nifi/shared';
import {
    Dimensions,
    FlowComparisonEntity,
    FlowUpdateRequestEntity,
    MoveComponentRequest,
    PasteRequest,
    SelectedComponent,
    Snippet,
    VersionControlInformationEntity
} from '../../../../state/flow-shared';
import {
    BacklogRequestEntity,
    BreadcrumbEntity,
    DisableComponentRequest,
    EnableComponentRequest,
    RegistryClientEntity,
    StartComponentRequest,
    StopComponentRequest,
    UpdateComponentRequest,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../state/shared';
import { BackNavigation } from '../../../../state/navigation';
import { BulletinEntity, ComponentType, ParameterContextReferenceEntity, Permissions, Revision } from '@nifi/shared';
import { VersionControlInformation } from '../../../../ui/common/tooltips/version-control-tip/version-control-tip.component';

export const flowFeatureKey = 'flowState';

export interface SelectComponentsRequest {
    components: SelectedComponent[];
}

export interface CenterComponentRequest {
    allowTransition: boolean;
}

/*
  Load Process Group
 */

export interface EnterProcessGroupRequest {
    id: string;
}

export interface LoadProcessGroupRequest {
    id: string;
    transitionRequired: boolean;
}

export interface LoadProcessGroupResponse {
    id: string;
    flow: ProcessGroupFlowEntity;
    flowStatus: ControllerStatusEntity;
    controllerBulletins: ControllerBulletinsEntity;
    connectedStateChanged: boolean;
    registryClients: RegistryClientEntity[];
}

export interface LoadConnectionSuccess {
    id: string;
    connection: any;
}

export interface LoadProcessorSuccess {
    id: string;
    processor: any;
}

export interface LoadInputPortSuccess {
    id: string;
    inputPort: any;
}

export interface LoadRemoteProcessGroupSuccess {
    id: string;
    remoteProcessGroup: any;
}

/*
  Component Requests
 */

export interface OpenSaveVersionDialogRequest {
    processGroupId: string;
    forceCommit?: boolean;
}

export interface OpenChangeVersionDialogRequest {
    processGroupId: string;
}

export interface ChangeVersionDialogRequest {
    processGroupId: string;
    revision: Revision;
    versionControlInformation: VersionControlInformation;
    versions: VersionedFlowSnapshotMetadataEntity[];
}

export interface SaveVersionDialogRequest {
    processGroupId: string;
    revision: Revision;
    registryClients?: RegistryClientEntity[];
    versionControlInformation?: VersionControlInformation;
    forceCommit?: boolean;
}

export interface ConfirmStopVersionControlRequest {
    processGroupId: string;
}

export interface StopVersionControlResponse {
    processGroupId: string;
    processGroupRevision: Revision;
}

export interface SaveVersionRequest {
    processGroupId: string;
    registry: string;
    bucket: string;
    flowName: string;
    revision: Revision;
    flowDescription?: string;
    comments?: string;
    existingFlowId?: string;
    branch?: string;
}

export interface RefreshRemoteProcessGroupRequest {
    id: string;
    refreshTimestamp: string;
}

export interface RefreshRemoteProcessGroupPollingDetailsRequest {
    request: RefreshRemoteProcessGroupRequest;
    polling: boolean;
}

export interface OpenComponentDialogRequest {
    id: string;
    type: ComponentType;
}

export interface NavigateToManageComponentPoliciesRequest {
    resource: string;
    id: string;
    backNavigation: BackNavigation;
}

export interface RpgManageRemotePortsRequest {
    id: string;
}

export interface NavigateToControllerServicesRequest {
    id: string;
}

export interface NavigateToQueueListing {
    connectionId: string;
}

export interface NavigateToParameterContext {
    id: string;
    backNavigation: BackNavigation;
}

export interface EditCurrentProcessGroupRequest {
    id: string;
}

export interface UpdatePositionsRequest {
    requestId: number;
    componentUpdates: UpdateComponentRequest[];
    connectionUpdates: UpdateComponentRequest[];
}

export interface MoveComponentsRequest {
    components: MoveComponentRequest[];
    groupId: string;
}

///////////////////////////////////////////////////////////

export interface PasteResponseEntity {
    flow: Flow;
    revision: Revision;
}

export interface PasteResponseContext extends PasteResponseEntity {
    pasteRequest: PasteRequest;
}

///////////////////////////////////////////////////////////

export interface DeleteComponentResponse {
    id: string;
    type: ComponentType;
}

export interface NavigateToComponentRequest {
    id: string;
    type: ComponentType;
    processGroupId?: string;
}

export interface NavigateToComponentsRequest {
    ids: string[];
    processGroupId?: string;
}

export type ConnectionDirection = 'upstream' | 'downstream';

export interface ViewComponentConnectionsRequest {
    // the id of the component whose connections are being requested
    id: string;
    // the name of the component, or its id when the current user cannot read it
    name: string;
    // the type of the component
    type: ComponentType;
    // the id of the group that defines the connections. this is the current group for every component
    // except an Input Port searched upstream or an Output Port searched downstream, whose connections
    // cross the enclosing group's boundary and are defined in its parent
    groupId: string;
    direction: ConnectionDirection;
}

export interface ComponentConnectionsDialogRequest {
    componentId: string;
    componentName: string;
    componentType: ComponentType;
    // the group the connections belong to, used when navigating to one of them
    groupId: string;
    direction: ConnectionDirection;
    connections: ConnectionEntity[];
    // names resolved from the same flow entity that supplied the connections
    groupIdToName: Map<string, string>;
    // names of the components the current user can read, across every group these connections reach
    // into. a connection is readable only when both of its ends are, so this is what lets each end be
    // reported on its own permission rather than through the connection that joins them
    componentIdToName: Map<string, string>;
}

/*
    Snippets
 */

export interface CopiedSnippet {
    snippet: Snippet;
    origin: Position;
    dimensions: any;
}

/*
  Application State
 */

export interface ComponentEntity {
    id: string;
    permissions: Permissions;
    position: Position;
    revision: Revision;
    component: any;
}

export interface ComponentEntityWithDimensions extends ComponentEntity {
    dimensions: Dimensions;
}

/**
 * A connection as returned by the flow endpoints. The source and destination are duplicated outside
 * of the permission gated `component` so that a connection the current user cannot read can still be
 * placed on the canvas. Prefer these fields over `component.source`/`component.destination` when the
 * connection may be unauthorized.
 */
export interface ConnectionEntity extends ComponentEntity {
    sourceId: string;
    sourceGroupId: string;
    sourceType: string;
    destinationId: string;
    destinationGroupId: string;
    destinationType: string;
}

export interface Flow {
    processGroups: ComponentEntity[];
    remoteProcessGroups: ComponentEntity[];
    processors: ComponentEntity[];
    inputPorts: ComponentEntity[];
    outputPorts: ComponentEntity[];
    connections: ConnectionEntity[];
    labels: ComponentEntity[];
    funnels: ComponentEntity[];
}

export type ResolvedExecutionEngine = 'STANDARD' | 'STATELESS';

export interface ProcessGroupFlow {
    id: string;
    uri: string;
    parentGroupId: string | null;
    breadcrumb: BreadcrumbEntity;
    parameterContext: ParameterContextReferenceEntity | null;
    flow: Flow;
    lastRefreshed: string;
    resolvedExecutionEngine: ResolvedExecutionEngine;
}

export interface ProcessGroupFlowEntity {
    permissions: Permissions;
    revision: Revision;
    processGroupFlow: ProcessGroupFlow;
}

export interface ControllerStatus {
    activeThreadCount: number;
    terminatedThreadCount: number;
    queued: string;
    flowFilesQueued: number;
    bytesQueued: number;
    runningCount: number;
    stoppedCount: number;
    invalidCount: number;
    disabledCount: number;
    activeRemotePortCount: number;
    inactiveRemotePortCount: number;
    upToDateCount?: number;
    locallyModifiedCount?: number;
    staleCount?: number;
    locallyModifiedAndStaleCount?: number;
    syncFailureCount?: number;
}

export interface ControllerStatusEntity {
    controllerStatus: ControllerStatus;
}

export interface ControllerBulletinsEntity {
    bulletins: BulletinEntity[];
    controllerServiceBulletins: BulletinEntity[];
    reportingTaskBulletins: BulletinEntity[];
    parameterProviderBulletins: BulletinEntity[];
    flowRegistryClientBulletins: BulletinEntity[];
}

export interface FlowState {
    id: string;
    flow: ProcessGroupFlowEntity;
    addedCache: string[];
    removedCache: string[];
    flowStatus: ControllerStatusEntity;
    refreshRpgDetails: RefreshRemoteProcessGroupPollingDetailsRequest | null;
    controllerBulletins: ControllerBulletinsEntity;
    registryClients: RegistryClientEntity[];
    dragging: boolean;
    transitionRequired: boolean;
    skipTransform: boolean;
    allowTransition: boolean;
    saving: boolean;
    navigationCollapsed: boolean;
    operationCollapsed: boolean;
    flowAnalysisOpen: boolean;
    versionSaving: boolean;
    changeVersionRequest: FlowUpdateRequestEntity | null;
    pollingProcessor: StartPollingProcessorUntilStoppedRequest | null;
    status: 'pending' | 'loading' | 'success' | 'complete';
}

export interface RunOnceResponse {
    component: ComponentEntity;
}

export interface EnableComponentsRequest {
    components: EnableComponentRequest[];
}

export interface EnableComponentResponse {
    type: ComponentType;
    component: ComponentEntity;
}

export interface EnableProcessGroupResponse {
    type: ComponentType;
    component: {
        id: string;
        state: string;
    };
}

export interface DisableComponentsRequest {
    components: DisableComponentRequest[];
}

export interface DisableComponentResponse {
    type: ComponentType;
    component: ComponentEntity;
}

export interface DisableProcessGroupResponse {
    type: ComponentType;
    component: {
        id: string;
        state: string;
    };
}

export interface StartComponentsRequest {
    components: StartComponentRequest[];
}

export interface StartComponentResponse {
    type: ComponentType;
    component: ComponentEntity;
}

export interface StartProcessGroupResponse {
    type: ComponentType;
    component: {
        id: string;
        state: string;
    };
}

export interface StopProcessGroupResponse {
    type: ComponentType;
    component: {
        id: string;
        state: string;
    };
}

export interface ComponentRunStatusRequest {
    revision: Revision;
    state: string;
    disconnectedNodeAcknowledged: boolean;
}

export interface StartPollingProcessorUntilStoppedRequest {
    id: string;
}

export interface StopSourcesRequest {
    id: string;
}

export interface StopSourcesResponse {
    id: string;
    state: 'STOPPED';
    components: Record<string, Revision>;
    disconnectedNodeAcknowledged?: boolean;
}

export interface StopComponentResponse {
    type: ComponentType;
    component: ComponentEntity;
}

export interface StopComponentsRequest {
    components: StopComponentRequest[];
}

export interface LoadChildProcessGroupRequest {
    id: string;
}

/*
  Clear Bulletins
*/

export interface ClearBulletinsForGroupResponse {
    processGroupId: string;
    bulletinsCleared: number;
}

export interface OpenLocalChangesDialogRequest {
    processGroupId: string;
}

export interface LocalChangesDialogRequest {
    versionControlInformation: VersionControlInformationEntity;
    localModifications: FlowComparisonEntity;
    mode: 'SHOW' | 'REVERT';
}

export interface ProcessorBacklogDialogRequest {
    processorId: string;
    requestEntity?: BacklogRequestEntity;
    errorMessage?: string;
}

export interface MoveToFrontRequest {
    componentType: ComponentType.Connection | ComponentType.Label;
    id: string;
    uri: string;
    revision: Revision;
    zIndex: number;
}

export interface ChangeColorRequest {
    id: string;
    uri: string;
    type: ComponentType;
    color: string | null;
    revision: Revision;
    style: any | null;
}
