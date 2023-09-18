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

import { ComponentType, Position } from '../shared';

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
}

export interface LoadProcessGroupResponse {
    id: string;
    flow: any;
}

/*
  Component Requests
 */

export interface CreateComponent {
    type: ComponentType;
    position: Position;
    revision: any;
}

export interface CreateComponentResponse {
    type: ComponentType;
    payload: any;
}

export interface CreatePort extends CreateComponent {
    name: string;
    allowRemoteAccess: boolean;
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

/*
    Tooltips
 */

export interface TextTipInput {
    text: string;
}

export interface UnorderedListTipInput {
    items: string[];
}

export interface ValidationErrorsTipInput {
    isValidating: boolean;
    validationErrors: string[];
}

export interface BulletinEntity {
    canRead: boolean;
    id: number;
    sourceId: string;
    groupId: string;
    timestamp: string;
    nodeAddress: string;
    bulletin: {
        id: number;
        sourceId: string;
        groupId: string;
        category: string;
        level: string;
        message: string;
        sourceName: string;
        timestamp: string;
        nodeAddress: string;
    };
}

export interface BulletinsTipInput {
    bulletins: BulletinEntity[];
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

export interface Permissions {
    canRead: boolean;
    canWrite: boolean;
}

export interface ComponentEntity {
    id: string;
    position: Position;
    component: any;
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
    breadcrumb: any;
    flow: Flow;
    lastRefreshed: string;
}

export interface ProcessGroupFlowEntity {
    permissions: Permissions;
    processGroupFlow: ProcessGroupFlow;
}

export interface FlowState {
    id: string;
    flow: ProcessGroupFlowEntity;
    dragging: boolean;
    transitionRequired: boolean;
    renderRequired: boolean;
    error: string | null;
    status: 'pending' | 'loading' | 'error' | 'success';
}
