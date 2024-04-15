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

import { BreadcrumbEntity, Position } from '../shared';
import {
    BulletinEntity,
    Bundle,
    ComponentType,
    DocumentedType,
    ParameterContextReferenceEntity,
    Permissions,
    RegistryClientEntity,
    Revision,
    SelectOption,
    SparseVersionedFlow,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../state/shared';
import { ParameterContextEntity } from '../../../parameter-contexts/state/parameter-context-listing';

export const flowFeatureKey = 'flowState';

export interface SelectedComponent {
    id: string;
    componentType: ComponentType;
    entity?: any;
}

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

export interface CreateComponentRequest {
    type: ComponentType;
    position: Position;
    revision: any;
}

export interface CreateConnectionRequest {
    source: SelectedComponent;
    destination: SelectedComponent;
}

export const loadBalanceStrategies: SelectOption[] = [
    {
        text: 'Do not load balance',
        value: 'DO_NOT_LOAD_BALANCE',
        description: 'Do not load balance FlowFiles between nodes in the cluster.'
    },
    {
        text: 'Partition by attribute',
        value: 'PARTITION_BY_ATTRIBUTE',
        description:
            'Determine which node to send a given FlowFile to based on the value of a user-specified FlowFile Attribute. ' +
            'All FlowFiles that have the same value for said Attribute will be sent to the same node in the cluster.'
    },
    {
        text: 'Round robin',
        value: 'ROUND_ROBIN',
        description:
            'FlowFiles will be distributed to nodes in the cluster in a Round-Robin fashion. However, if a node in the ' +
            'cluster is not able to receive data as fast as other nodes, that node may be skipped in one or more iterations ' +
            'in order to maximize throughput of data distribution across the cluster.'
    },
    {
        text: 'Single node',
        value: 'SINGLE_NODE',
        description: 'All FlowFiles will be sent to the same node. Which node they are sent to is not defined.'
    }
];

export const loadBalanceCompressionStrategies: SelectOption[] = [
    {
        text: 'Do not compress',
        value: 'DO_NOT_COMPRESS',
        description: 'FlowFiles will not be compressed'
    },
    {
        text: 'Compress attributes only',
        value: 'COMPRESS_ATTRIBUTES_ONLY',
        description: "FlowFiles' attributes will be compressed, but the FlowFiles' contents will not be"
    },
    {
        text: 'Compress attributes and content',
        value: 'COMPRESS_ATTRIBUTES_AND_CONTENT',
        description: "FlowFiles' attributes and content will be compressed"
    }
];

export interface CreateConnectionDialogRequest {
    request: CreateConnectionRequest;
    defaults: {
        flowfileExpiration: string;
        objectThreshold: number;
        dataSizeThreshold: string;
    };
}

export interface CreateConnection {
    payload: any;
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
    payload: any;
    keepExistingParameterContext: boolean;
}

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

export interface SaveToVersionControlRequest {
    processGroupId: string;
    versionedFlow: SparseVersionedFlow;
    processGroupRevision: Revision;
}

export interface ConfirmStopVersionControlRequest {
    processGroupId: string;
}

export interface StopVersionControlRequest {
    revision: Revision;
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
}

export interface VersionControlInformation {
    groupId: string;
    registryId: string;
    registryName: string;
    bucketId: string;
    bucketName: string;
    flowId: string;
    flowName: string;
    flowDescription: string;
    version: number;
    storageLocation?: string;
    state: string;
    stateExplanation: string;
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
    processorTypes: DocumentedType[];
}

export interface GoToRemoteProcessGroupRequest {
    uri: string;
}

export interface RefreshRemoteProcessGroupRequest {
    id: string;
    refreshTimestamp: string;
}

export interface RefreshRemoteProcessGroupPollingDetailsRequest {
    request: RefreshRemoteProcessGroupRequest;
    polling: boolean;
}

export interface CreateProcessorRequest extends CreateComponentRequest {
    processorType: string;
    processorBundle: Bundle;
}

export interface CreateProcessGroupRequest extends CreateComponentRequest {
    name: string;
    parameterContextId: string;
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

export interface OpenComponentDialogRequest {
    id: string;
    type: ComponentType;
}

export interface NavigateToManageComponentPoliciesRequest {
    resource: string;
    id: string;
}

export interface EditComponentDialogRequest {
    type: ComponentType;
    uri: string;
    entity: any;
}

export interface EditRemotePortDialogRequest extends EditComponentDialogRequest {
    rpg?: any;
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

export interface EditCurrentProcessGroupRequest {
    id: string;
}

export interface EditConnectionDialogRequest extends EditComponentDialogRequest {
    newDestination?: {
        type: ComponentType | null;
        id?: string;
        groupId: string;
        name: string;
    };
}

export interface UpdateProcessorRequest {
    payload: any;
    postUpdateNavigation?: string[];
}

export interface UpdateComponentRequest {
    requestId?: number;
    id: string;
    type: ComponentType;
    uri: string;
    payload: any;
    restoreOnFailure?: any;
    postUpdateNavigation?: string[];
}

export interface UpdateComponentResponse {
    requestId?: number;
    id: string;
    type: ComponentType;
    response: any;
    postUpdateNavigation?: string[];
}

export interface UpdateComponentFailure {
    error: string;
    id: string;
    type: ComponentType;
    restoreOnFailure?: any;
}

export interface UpdateConnectionRequest extends UpdateComponentRequest {
    previousDestination?: any;
}

export interface UpdateConnectionSuccess extends UpdateComponentResponse {
    previousDestination?: any;
}

export interface UpdatePositionsRequest {
    requestId: number;
    componentUpdates: UpdateComponentRequest[];
    connectionUpdates: UpdateComponentRequest[];
}

export interface MoveComponentRequest {
    id: string;
    uri: string;
    type: ComponentType;
    entity: any;
}

export interface MoveComponentsRequest {
    components: MoveComponentRequest[];
    groupId: string;
}

export interface DeleteComponentRequest {
    id: string;
    uri: string;
    type: ComponentType;
    entity: any;
}

export interface DeleteComponentResponse {
    id: string;
    type: ComponentType;
}

export interface NavigateToComponentRequest {
    id: string;
    type: ComponentType;
    processGroupId?: string;
}

export interface ReplayLastProvenanceEventRequest {
    componentId: string;
    nodes: string;
}

/*
    Snippets
 */

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

/*
    Tooltips
 */

export interface VersionControlInformation {
    groupId: string;
    registryId: string;
    registryName: string;
    bucketId: string;
    bucketName: string;
    flowId: string;
    flowName: string;
    flowDescription: string;
    version: number;
    state: string;
    stateExplanation: string;
    storageLocation?: string;
}

export interface VersionControlTipInput {
    versionControlInformation: VersionControlInformation;
}

/*
  Application State
 */

export interface ComponentEntity {
    id: string;
    permissions: Permissions;
    position: Position;
    component: any;
}

export interface Relationship {
    autoTerminate: boolean;
    description: string;
    name: string;
    retry: boolean;
}

export interface Flow {
    processGroups: ComponentEntity[];
    remoteProcessGroups: ComponentEntity[];
    processors: ComponentEntity[];
    inputPorts: ComponentEntity[];
    outputPorts: ComponentEntity[];
    connections: ComponentEntity[];
    labels: ComponentEntity[];
    funnels: ComponentEntity[];
}

export interface ProcessGroupFlow {
    id: string;
    uri: string;
    parentGroupId: string | null;
    breadcrumb: BreadcrumbEntity;
    parameterContext: ParameterContextReferenceEntity | null;
    flow: Flow;
    lastRefreshed: string;
}

export interface ProcessGroupFlowEntity {
    permissions: Permissions;
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
    flowStatus: ControllerStatusEntity;
    refreshRpgDetails: RefreshRemoteProcessGroupPollingDetailsRequest | null;
    controllerBulletins: ControllerBulletinsEntity;
    dragging: boolean;
    transitionRequired: boolean;
    skipTransform: boolean;
    allowTransition: boolean;
    saving: boolean;
    navigationCollapsed: boolean;
    operationCollapsed: boolean;
    error: string | null;
    versionSaving: boolean;
    changeVersionRequest: FlowUpdateRequestEntity | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}

export interface RunOnceRequest {
    uri: string;
    revision: Revision;
}

export interface RunOnceResponse {
    component: ComponentEntity;
}

export interface StartProcessGroupRequest {
    id: string;
    type: ComponentType;
}

export interface StartComponentRequest {
    id: string;
    uri: string;
    type: ComponentType;
    revision: Revision;
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

export interface StartComponentsResponse {
    components: StartComponentsResponse[];
}

export interface ComponentRunStatusRequest {
    revision: Revision;
    state: string;
    disconnectedNodeAcknowledged: boolean;
}

export interface ProcessGroupRunStatusRequest {
    id: string;
    state: string;
    disconnectedNodeAcknowledged: boolean;
}

export interface StopComponentRequest {
    id: string;
    uri: string;
    type: ComponentType;
    revision: Revision;
}

export interface StopProcessGroupRequest {
    id: string;
    type: ComponentType;
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

export interface OpenLocalChangesDialogRequest {
    processGroupId: string;
}

export interface LocalChangesDialogRequest {
    versionControlInformation: VersionControlInformationEntity;
    localModifications: FlowComparisonEntity;
    mode: 'SHOW' | 'REVERT';
}

export interface DownloadFlowRequest {
    processGroupId: string;
    includeReferencedServices: boolean;
}
