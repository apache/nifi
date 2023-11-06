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

import { Position } from '../shared';
import { BulletinEntity, Bundle, ComponentType, DocumentedType, Permissions } from '../../../../state/shared';
import { ParameterContextEntity } from '../../../parameter-contexts/state/parameter-context-listing';

export const flowFeatureKey = 'flowState';

export interface SelectedComponent {
    id: string;
    componentType: ComponentType;
    entity?: any;
}

export interface SelectComponents {
    components: SelectedComponent[];
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
    clusterSummary: ClusterSummary;
    controllerBulletins: ControllerBulletinsEntity;
}

export interface LoadConnectionSuccess {
    id: string;
    connection: any;
}

/*
  Component Requests
 */

export interface CreateComponent {
    type: ComponentType;
    position: Position;
    revision: any;
}

export interface CreateConnectionRequest {
    source: SelectedComponent;
    destination: SelectedComponent;
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
    payload: any;
}

export interface CreateProcessGroupDialogRequest {
    request: CreateComponent;
    parameterContexts: ParameterContextEntity[];
}

export interface CreateProcessorDialogRequest {
    request: CreateComponent;
    processorTypes: DocumentedType[];
}

export interface CreateProcessor extends CreateComponent {
    processorType: string;
    processorBundle: Bundle;
}

export interface CreateProcessGroup extends CreateComponent {
    name: string;
    parameterContextId: string;
}

export interface UploadProcessGroup extends CreateComponent {
    name: string;
    flowDefinition: File;
}

export interface CreatePort extends CreateComponent {
    name: string;
    allowRemoteAccess: boolean;
}

export interface CreateComponentResponse {
    type: ComponentType;
    payload: any;
}

export interface EditComponentRequest {
    id: string;
    type: ComponentType;
}

export interface EditComponent {
    type: ComponentType;
    uri: string;
    entity: any;
}

export interface UpdateComponent {
    requestId?: number;
    id: string;
    type: ComponentType;
    uri: string;
    payload: any;
    restoreOnFailure?: any;
}

export interface UpdateComponentResponse {
    requestId?: number;
    id: string;
    type: ComponentType;
    response: any;
}

export interface UpdateComponentFailure {
    error: string;
    id: string;
    type: ComponentType;
    restoreOnFailure?: any;
}

export interface UpdatePositions {
    requestId: number;
    componentUpdates: UpdateComponent[];
    connectionUpdates: UpdateComponent[];
}

export interface DeleteComponent {
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
    storageLocation: string;
    state: string;
    stateExplanation: string;
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

export interface Breadcrumb {
    id: string;
    name: string;
    versionControlInformation?: VersionControlInformation;
}

export interface BreadcrumbEntity {
    id: string;
    permissions: Permissions;
    versionedFlowState: string;
    breadcrumb: Breadcrumb;
    parentBreadcrumb?: BreadcrumbEntity;
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

export interface ClusterSummary {
    clustered: boolean;
    connectedToCluster: boolean;
    connectedNodes?: string;
    connectedNodeCount: number;
    totalNodeCount: number;
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
    clusterSummary: ClusterSummary;
    controllerBulletins: ControllerBulletinsEntity;
    dragging: boolean;
    transitionRequired: boolean;
    skipTransform: boolean;
    saving: boolean;
    navigationCollapsed: boolean;
    operationCollapsed: boolean;
    error: string | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}
