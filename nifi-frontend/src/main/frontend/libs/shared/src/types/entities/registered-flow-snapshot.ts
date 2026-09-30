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

import { Bundle, Position } from '../rest-api.types';

/**
 * Fully typed graph reachable from ProcessGroupEntity.versionedFlowSnapshot.
 *
 * @nifi-source: nifi-api/src/main/java/org/apache/nifi/registry/flow/RegisteredFlowSnapshot.java
 * @nifi-revision: apache/nifi-api@dc575b83e2c0cddf6ac6d3adc51d3448a70654eb
 * @vetted: 2026-09-29
 */
export interface RegisteredFlowSnapshot {
    snapshotMetadata?: RegisteredFlowSnapshotMetadata;
    flow?: RegisteredFlow;
    bucket?: FlowRegistryBucket;
    flowContents?: VersionedProcessGroup;
    externalControllerServices?: Record<string, ExternalControllerServiceReference>;
    parameterContexts?: Record<string, VersionedParameterContext>;
    flowEncodingVersion?: string;
    parameterProviders?: Record<string, ParameterProviderReference>;
    latest: boolean;
}

export interface RegisteredFlowSnapshotMetadata {
    registryIdentifier?: string;
    registryName?: string;
    branch?: string;
    bucketIdentifier?: string;
    flowIdentifier?: string;
    flowName?: string;
    version?: string;
    timestamp: number;
    author?: string;
    comments?: string;
}

export interface RegisteredFlow {
    identifier?: string;
    name?: string;
    description?: string;
    branch?: string;
    bucketIdentifier?: string;
    bucketName?: string;
    createdTimestamp: number;
    lastModifiedTimestamp: number;
    permissions?: FlowRegistryPermissions;
    versionCount: number;
    versionInfo?: RegisteredFlowVersionInfo;
}

export interface RegisteredFlowVersionInfo {
    version: number;
}

export interface FlowRegistryBucket {
    identifier?: string;
    name?: string;
    description?: string;
    createdTimestamp: number;
    permissions?: FlowRegistryPermissions;
}

export interface FlowRegistryPermissions {
    canRead: boolean;
    canWrite: boolean;
    canDelete: boolean;
}

export interface ExternalControllerServiceReference {
    identifier?: string;
    name?: string;
}

export interface ParameterProviderReference {
    identifier?: string;
    name?: string;
    type?: string;
    bundle?: Bundle;
}

export type VersionedComponentType =
    | 'PROCESSOR'
    | 'CONTROLLER_SERVICE'
    | 'PROCESS_GROUP'
    | 'REMOTE_PROCESS_GROUP'
    | 'INPUT_PORT'
    | 'OUTPUT_PORT'
    | 'CONNECTION'
    | 'FUNNEL'
    | 'LABEL'
    | 'REPORTING_TASK'
    | 'FLOW_REGISTRY_CLIENT'
    | 'PARAMETER_PROVIDER'
    | 'FLOW_ANALYSIS_RULE'
    | 'PARAMETER_CONTEXT'
    | 'CONNECTOR'
    | 'REMOTE_INPUT_PORT'
    | 'REMOTE_OUTPUT_PORT';

export interface VersionedComponent {
    identifier?: string;
    instanceIdentifier?: string;
    groupIdentifier?: string;
    name?: string;
    comments?: string;
    position?: Position;
    componentType: VersionedComponentType;
}

export interface VersionedConfigurableExtension extends VersionedComponent {
    type?: string;
    bundle?: Bundle;
    properties?: Record<string, string | null>;
    propertyDescriptors?: Record<string, VersionedPropertyDescriptor>;
    componentState?: VersionedComponentState;
}

export interface VersionedProcessGroup extends VersionedComponent {
    componentType: 'PROCESS_GROUP';
    processGroups: VersionedProcessGroup[];
    remoteProcessGroups: VersionedRemoteProcessGroup[];
    processors: VersionedProcessor[];
    inputPorts: VersionedPort[];
    outputPorts: VersionedPort[];
    connections: VersionedConnection[];
    labels: VersionedLabel[];
    funnels: VersionedFunnel[];
    controllerServices: VersionedControllerService[];
    versionedFlowCoordinates?: VersionedFlowCoordinates;
    parameterContextName?: string;
    flowFileConcurrency?: string;
    flowFileOutboundPolicy?: string;
    defaultFlowFileExpiration?: string;
    defaultBackPressureObjectThreshold?: number;
    defaultBackPressureDataSizeThreshold?: string;
    scheduledState?: ScheduledState;
    executionEngine?: ExecutionEngine;
    maxConcurrentTasks?: number;
    statelessFlowTimeout?: string;
    statelessFlowFileContentInMemoryMax?: string;
    statelessFlowFileContentInMemoryHeapPercentage?: number;
    logFileSuffix?: string;
}

export interface VersionedFlowCoordinates {
    registryId?: string;
    storageLocation?: string;
    branch?: string;
    bucketId?: string;
    flowId?: string;
    version?: string;
    latest?: boolean;
}

export type ScheduledState = 'ENABLED' | 'DISABLED' | 'RUNNING';
export type ExecutionEngine = 'STATELESS' | 'STANDARD' | 'INHERITED';

export interface VersionedProcessor extends VersionedConfigurableExtension {
    componentType: 'PROCESSOR';
    style?: Record<string, string>;
    annotationData?: string;
    schedulingPeriod?: string;
    schedulingStrategy?: string;
    executionNode?: string;
    penaltyDuration?: string;
    yieldDuration?: string;
    bulletinLevel?: string;
    runDurationMillis?: number;
    concurrentlySchedulableTaskCount?: number;
    autoTerminatedRelationships?: string[];
    scheduledState?: ScheduledState;
    retryCount?: number;
    retriedRelationships?: string[];
    backoffMechanism?: string;
    maxBackoffPeriod?: string;
}

export interface VersionedPort extends VersionedComponent {
    componentType: 'INPUT_PORT' | 'OUTPUT_PORT';
    type?: 'INPUT_PORT' | 'OUTPUT_PORT';
    concurrentlySchedulableTaskCount?: number;
    scheduledState?: ScheduledState;
    allowRemoteAccess?: boolean;
    portFunction?: 'STANDARD' | 'FAILURE';
}

export interface VersionedConnection extends VersionedComponent {
    componentType: 'CONNECTION';
    source?: VersionedConnectableComponent;
    destination?: VersionedConnectableComponent;
    labelIndex?: number;
    zIndex?: number;
    selectedRelationships?: string[];
    backPressureObjectThreshold?: number;
    backPressureDataSizeThreshold?: string;
    flowFileExpiration?: string;
    prioritizers?: string[];
    bends?: Position[];
    loadBalanceStrategy?: string;
    partitioningAttribute?: string;
    loadBalanceCompression?: string;
}

export interface VersionedConnectableComponent {
    id?: string;
    instanceIdentifier?: string;
    type?: VersionedConnectableComponentType;
    groupId?: string;
    name?: string;
    comments?: string;
}

export type VersionedConnectableComponentType =
    | 'PROCESSOR'
    | 'REMOTE_INPUT_PORT'
    | 'REMOTE_OUTPUT_PORT'
    | 'INPUT_PORT'
    | 'OUTPUT_PORT'
    | 'FUNNEL';

export interface VersionedRemoteProcessGroup extends VersionedComponent {
    componentType: 'REMOTE_PROCESS_GROUP';
    targetUris?: string;
    communicationsTimeout?: string;
    yieldDuration?: string;
    transportProtocol?: string;
    localNetworkInterface?: string;
    proxyHost?: string;
    proxyPort?: number;
    proxyUser?: string;
    proxyPassword?: string;
    inputPorts?: VersionedRemoteGroupPort[];
    outputPorts?: VersionedRemoteGroupPort[];
}

export interface VersionedRemoteGroupPort extends VersionedComponent {
    componentType: 'REMOTE_INPUT_PORT' | 'REMOTE_OUTPUT_PORT';
    remoteGroupId?: string;
    concurrentlySchedulableTaskCount?: number;
    useCompression?: boolean;
    batchSize?: VersionedBatchSize;
    targetId?: string;
    scheduledState?: ScheduledState;
}

export interface VersionedBatchSize {
    count?: number;
    size?: string;
    duration?: string;
}

export interface VersionedLabel extends VersionedComponent {
    componentType: 'LABEL';
    label?: string;
    zIndex?: number;
    width?: number;
    height?: number;
    style?: Record<string, string>;
}

export interface VersionedFunnel extends VersionedComponent {
    componentType: 'FUNNEL';
}

export interface VersionedControllerService extends VersionedConfigurableExtension {
    componentType: 'CONTROLLER_SERVICE';
    controllerServiceApis?: VersionedControllerServiceApi[];
    annotationData?: string;
    scheduledState?: ScheduledState;
    bulletinLevel?: string;
}

export interface VersionedControllerServiceApi {
    type?: string;
    bundle?: Bundle;
}

export interface VersionedPropertyDescriptor {
    name?: string;
    displayName?: string;
    identifiesControllerService: boolean;
    sensitive: boolean;
    dynamic: boolean;
    resourceDefinition?: VersionedResourceDefinition;
    listenPortDefinition?: VersionedListenPortDefinition;
}

export interface VersionedResourceDefinition {
    cardinality?: 'SINGLE' | 'MULTIPLE';
    resourceTypes?: Array<'FILE' | 'DIRECTORY' | 'TEXT' | 'URL'>;
}

export interface VersionedListenPortDefinition {
    transportProtocol?: 'TCP' | 'UDP';
    applicationProtocols?: string[];
}

export interface VersionedComponentState {
    clusterState?: Record<string, string>;
    localNodeStates?: Array<VersionedNodeState | null>;
}

export interface VersionedNodeState {
    state?: Record<string, string>;
}

export interface VersionedParameterContext extends VersionedComponent {
    componentType: 'PARAMETER_CONTEXT';
    name?: string;
    parameters?: VersionedParameter[];
    inheritedParameterContexts?: string[];
    description?: string;
    parameterProvider?: string;
    parameterGroupName?: string;
    synchronized?: boolean;
}

export interface VersionedParameter {
    name?: string;
    description?: string;
    sensitive: boolean;
    provided: boolean;
    value?: string;
    referencedAssets?: VersionedAsset[];
}

export interface VersionedAsset {
    identifier?: string;
    name?: string;
}
