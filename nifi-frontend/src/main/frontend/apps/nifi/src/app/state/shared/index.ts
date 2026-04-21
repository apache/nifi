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

import { HttpErrorResponse } from '@angular/common/http';
import { Observable } from 'rxjs';
import { GarbageCollection } from '../system-diagnostics';
import {
    AffectedComponentEntity,
    BulletinEntity,
    Bundle,
    ComponentType,
    Parameter,
    ParameterContextReferenceEntity,
    Permissions,
    Revision,
    SelectOption
} from '@nifi/shared';
import { VersionControlInformation } from '../../ui/common/tooltips/version-control-tip/version-control-tip.component';

export interface OkDialogRequest {
    title: string;
    message: string;
}

export interface CancelDialogRequest {
    title: string;
    message: string;
}

export interface NewPropertyDialogRequest {
    existingProperties: string[];
    allowsSensitive: boolean;
}

export interface NewPropertyDialogResponse {
    name: string;
    sensitive: boolean;
}

export interface EditParameterRequest {
    existingParameters?: string[];
    parameter?: Parameter;
    isNewParameterContext: boolean;
    isConvert?: boolean;
}

export interface EditParameterResponse {
    parameter: Parameter;
    valueChanged: boolean;
}

export interface AdvancedUiParams {
    url: string;
    id: string;
    revision: number;
    clientId: string;
    editable: boolean;
    disconnectedNodeAcknowledged: boolean;
}

export interface UserEntity {
    id: string;
    permissions: Permissions;
    component: User;
    revision: Revision;
    uri: string;
}

export interface User extends Tenant {
    userGroups: TenantEntity[];
    accessPolicies: AccessPolicySummaryEntity[];
}

export interface UserGroupEntity {
    id: string;
    permissions: Permissions;
    component: UserGroup;
    revision: Revision;
    uri: string;
}

export interface UserGroup extends Tenant {
    users: TenantEntity[];
    accessPolicies: AccessPolicySummaryEntity[];
}

export interface TenantEntity {
    id: string;
    revision: Revision;
    permissions: Permissions;
    component: Tenant;
}

export interface Tenant {
    id: string;
    identity: string;
    configurable: boolean;
}

export interface EditTenantRequest {
    user?: UserEntity;
    userGroup?: UserGroupEntity;
    existingUsers: UserEntity[];
    existingUserGroups: UserGroupEntity[];
}

export interface EditTenantResponse {
    revision: Revision;
    user?: any;
    userGroup?: any;
}

export interface EditControllerServiceDialogRequest {
    id: string;
    controllerService: ControllerServiceEntity;
    history?: ComponentHistory;
}

export interface EditComponentDialogRequest {
    type: ComponentType;
    uri: string;
    entity: any;
    history?: ComponentHistory;
    parameterContexts?: ParameterContextEntity[];
}

export interface EditRemotePortDialogRequest extends EditComponentDialogRequest {
    rpg?: any;
}

export interface EditConnectionDialogRequest extends EditComponentDialogRequest {
    newDestination?: {
        type: ComponentType | null;
        id?: string;
        groupId: string;
        name: string;
    };
}

export interface UpdateComponentRequest {
    requestId?: number;
    id: string;
    type: ComponentType;
    uri: string;
    payload: any;
    errorStrategy: 'snackbar' | 'banner';
    restoreOnFailure?: any;
}

export interface UpdateComponentResponse {
    requestId?: number;
    id: string;
    type: ComponentType;
    response: any;
}

export interface UpdateComponentFailure {
    errorResponse: HttpErrorResponse;
    id: string;
    type: ComponentType;
    errorStrategy: 'snackbar' | 'banner';
    restoreOnFailure?: any;
}

export interface UpdateProcessorRequest extends UpdateComponentRequest {
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface UpdateProcessorResponse extends UpdateComponentResponse {
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface UpdateConnectionRequest extends UpdateComponentRequest {
    previousDestination?: any;
}

export interface UpdateConnectionSuccess extends UpdateComponentResponse {
    previousDestination?: any;
}

export interface StartComponentRequest {
    id: string;
    type: ComponentType;
    revision: Revision;
    errorStrategy: 'snackbar' | 'banner';
}

export interface StopComponentRequest {
    id: string;
    type: ComponentType;
    revision: Revision;
    errorStrategy: 'snackbar' | 'banner';
}

export interface EnableComponentRequest {
    id: string;
    type: ComponentType;
    revision: Revision;
    errorStrategy: 'snackbar' | 'banner';
}

export interface DisableComponentRequest {
    id: string;
    type: ComponentType;
    revision: Revision;
    errorStrategy: 'snackbar' | 'banner';
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

export interface UpdateControllerServiceRequest {
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface SetEnableControllerServiceDialogRequest {
    id: string;
    controllerService: ControllerServiceEntity;
}

export interface DisableControllerServiceDialogRequest {
    id: string;
    controllerService: ControllerServiceEntity;
}

export interface ProvenanceEventSummary {
    id: string;
    eventId: number;
    eventTime: string;
    eventTimestamp: string;
    eventType: string;
    flowFileUuid: string;
    fileSize: string;
    fileSizeBytes: number;
    clusterNodeId?: string;
    clusterNodeAddress?: string;
    groupId: string;
    componentId: string;
    componentType: string;
    componentName: string;
}

export interface Attribute {
    name: string;
    value: string;
    previousValue: string;
}

export interface ProvenanceEvent extends ProvenanceEventSummary {
    eventDuration: string;
    lineageDuration: number;
    clusterNodeId: string;
    clusterNodeAddress: string;
    sourceSystemFlowFileId: string;
    alternateIdentifierUri: string;
    attributes?: Attribute[];
    parentUuids: string[];
    childUuids: string[];
    transitUri: string;
    relationship: string;
    details: string;
    contentEqual: boolean;
    inputContentAvailable: boolean;
    inputContentClaimSection: string;
    inputContentClaimContainer: string;
    inputContentClaimIdentifier: string;
    inputContentClaimOffset: number;
    inputContentClaimFileSize: string;
    inputContentClaimFileSizeBytes: number;
    outputContentAvailable: boolean;
    outputContentClaimSection: string;
    outputContentClaimContainer: string;
    outputContentClaimIdentifier: string;
    outputContentClaimOffset: string;
    outputContentClaimFileSize: string;
    outputContentClaimFileSizeBytes: number;
    replayAvailable: boolean;
    replayExplanation: string;
    sourceConnectionIdentifier: string;
}

export interface ProvenanceEventDialogRequest {
    event: ProvenanceEvent;
}

export interface PreviousValue {
    previousValue: string;
    timestamp: string;
    userIdentity: string;
}

export interface PropertyHistory {
    previousValues: PreviousValue[];
}

export interface ComponentHistory {
    componentId: string;
    propertyHistory: { [key: string]: PropertyHistory };
}

export interface ComponentHistoryEntity {
    componentHistory: ComponentHistory;
}

export interface UnorderedListTipInput {
    items: string[];
}

export interface SearchMatchTipInput {
    matches: string[];
}

export interface SearchResultGroup {
    id: string;
    name: string;
}

export interface ComponentSearchResult {
    id: string;
    groupId: string;
    parentGroup: SearchResultGroup;
    versionedGroup: SearchResultGroup;
    name: string;
    matches: string[];
}

export interface SearchResults {
    processorResults: ComponentSearchResult[];
    connectionResults: ComponentSearchResult[];
    processGroupResults: ComponentSearchResult[];
    inputPortResults: ComponentSearchResult[];
    outputPortResults: ComponentSearchResult[];
    remoteProcessGroupResults: ComponentSearchResult[];
    funnelResults: ComponentSearchResult[];
    labelResults: ComponentSearchResult[];
    controllerServiceNodeResults: ComponentSearchResult[];
    parameterContextResults: ComponentSearchResult[];
    parameterProviderNodeResults: ComponentSearchResult[];
    parameterResults: ComponentSearchResult[];
}

export interface SearchResultsEntity {
    searchResultsDTO: SearchResults;
}

export interface ControllerServiceApi {
    type: string;
    bundle: Bundle;
}

export interface ControllerServiceApiTipInput {
    controllerServiceApis: ControllerServiceApi[];
}

export interface ValidationErrorsTipInput {
    isValidating: boolean;
    validationErrors: string[];
}

export interface BulletinsTipInput {
    bulletins: BulletinEntity[];
}

export interface PropertyValueTipInput {
    parameters: ParameterEntity[];
    property: Property;
}

export interface PropertyTipInput {
    descriptor: PropertyDescriptor;
    propertyHistory?: PropertyHistory;
}

export interface RestrictionsTipInput {
    usageRestriction: string;
    explicitRestrictions: ExplicitRestriction[];
}

export interface GarbageCollectionTipInput {
    garbageCollections: GarbageCollection[];
}

export interface ExplicitRestriction {
    requiredPermission: RequiredPermission;
    explanation: string;
}

export interface RequiredPermission {
    id: string;
    label: string;
}

export interface ParameterEntity {
    canWrite?: boolean;
    parameter: Parameter;
}

export interface ParameterContextEntity {
    revision: Revision;
    permissions: Permissions;
    id: string;
    uri: string;
    component?: ParameterContext;
}

export interface ParameterContext {
    id: string;
    name: string;
    description: string;
    parameters: ParameterEntity[];
    boundProcessGroups: BoundProcessGroup[];
    inheritedParameterContexts: ParameterContextReferenceEntity[];
    parameterProviderConfiguration?: ParameterProviderConfigurationEntity;
}

export interface ConvertToParameterResponse {
    propertyValue: string;
    parameterContext?: ParameterContext;
}

// TODO - Replace this with ProcessGroupEntity was available
export interface BoundProcessGroup {
    permissions: Permissions;
    id: string;
    component: any;
}

export interface ParameterConfig {
    supportsParameters: boolean;
    parameters: Parameter[] | null;
}

export interface SubmitParameterContextUpdate {
    id: string;
    payload: any;
}

export interface PollParameterContextUpdateSuccess {
    requestEntity: ParameterContextUpdateRequestEntity;
}

export interface ParameterContextUpdateRequest {
    complete: boolean;
    lastUpdated: string;
    percentComponent: number;
    referencingComponents: AffectedComponentEntity[];
    requestId: string;
    state: string;
    updateSteps: any[];
    uri: string;
    parameterContext?: any;
    failureReason?: string;
}

export interface ParameterContextUpdateRequestEntity {
    parameterContextRevision: Revision;
    request: ParameterContextUpdateRequest;
}

export interface ControllerServiceReferencingComponent {
    groupId: string;
    id: string;
    name: string;
    type: string;
    state: string;
    properties: { [key: string]: string };
    descriptors: { [key: string]: PropertyDescriptor };
    validationErrors: string[];
    referenceType: string;
    activeThreadCount?: number;
    referenceCycle: boolean;
    referencingComponents: ControllerServiceReferencingComponentEntity[];
}

export interface ControllerServiceReferencingComponentEntity {
    permissions: Permissions;
    bulletins: BulletinEntity[];
    operatePermissions: Permissions;
    component: ControllerServiceReferencingComponent;
}

export interface ControllerServiceEntity {
    permissions: Permissions;
    operatePermissions?: Permissions;
    revision: Revision;
    bulletins: BulletinEntity[];
    id: string;
    parentGroupId?: string;
    uri: string;
    status: any;
    component: any;
}

export interface AccessPolicySummaryEntity {
    id: string;
    component: AccessPolicySummary;
    revision: Revision;
    permissions: Permissions;
}

export interface AccessPolicySummary {
    id: string;
    resource: string;
    action: string;
    componentReference?: ComponentReferenceEntity;
    configurable: boolean;
}

export interface ComponentReferenceEntity {
    id: string;
    parentGroupId?: string;
    component: ComponentReference;
    revision: Revision;
    permissions: Permissions;
}

export interface ComponentReference {
    id: string;
    parentGroupId?: string;
    name: string;
}

export interface DocumentedType {
    bundle: Bundle;
    description?: string;
    restricted: boolean;
    tags: string[];
    type: string;
    controllerServiceApis?: ControllerServiceApi[];
    explicitRestrictions?: ExplicitRestriction[];
    usageRestriction?: string;
    deprecationReason?: string;
}

export interface AllowableValue {
    displayName: string;
    value: string | null;
    description?: string;
}

export interface AllowableValueEntity {
    canRead: boolean;
    allowableValue: AllowableValue;
}

export interface RegistryClientEntity {
    permissions: Permissions;
    operatePermissions?: Permissions;
    revision: Revision;
    bulletins?: BulletinEntity[];
    id: string;
    uri: string;
    component: any;
}

export interface BucketEntity {
    id: string;
    permissions: Permissions;
    bucket: Bucket;
}

export interface Bucket {
    created: number;
    description: string;
    id: string;
    name: string;
}

export interface BranchEntity {
    id: string;
    permissions: Permissions;
    branch: Branch;
}

export interface Branch {
    name: string;
}

export interface VersionedFlowEntity {
    versionedFlow: VersionedFlow;
}

export interface VersionedFlow {
    registryId: string;
    bucketId: string;
    flowId?: string;
    flowName: string;
    description: string;
    comments: string;
    action: string;
    branch?: string;
}

export interface SparseVersionedFlow {
    registryId: string;
    bucketId: string;
    action: string;
    comments?: string;
    flowId?: string;
    flowName?: string;
    description?: string;
    branch?: string;
}

export interface VersionedFlowSnapshotMetadataEntity {
    registryId: string;
    versionedFlowSnapshotMetadata: VersionedFlowSnapshotMetadata;
}

export interface VersionedFlowSnapshotMetadata {
    bucketIdentifier: string;
    flowIdentifier: string;
    version: string;
    timestamp: number;
    author: string;
    comments: string;
    branch?: string;
}

export interface PropertyDependency {
    propertyName: string;
    dependentValues: string[];
}

export interface PropertyDescriptor {
    name: string;
    displayName: string;
    description: string;
    defaultValue?: string;
    allowableValues?: AllowableValueEntity[];
    required: boolean;
    sensitive: boolean;
    dynamic: boolean;
    supportsEl: boolean;
    expressionLanguageScope: string;
    dependencies: PropertyDependency[];
    identifiesControllerService?: string;
    identifiesControllerServiceBundle?: Bundle;
}

export interface PropertyDescriptorEntity {
    propertyDescriptor: PropertyDescriptor;
}

export interface Property {
    property: string;
    value: string | null;
    descriptor: PropertyDescriptor;
}

export interface InlineServiceCreationRequest {
    descriptor: PropertyDescriptor;
}

export interface InlineServiceCreationResponse {
    value: string;
    descriptor: PropertyDescriptor;
}

export interface PropertyDescriptorRetriever {
    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<PropertyDescriptorEntity>;
}

export interface CreateControllerServiceRequest {
    processGroupId?: string;
    controllerServiceType: string;
    controllerServiceBundle: Bundle;
    revision: Revision;
}

export interface ControllerServiceCreator {
    createControllerService(createControllerService: CreateControllerServiceRequest): Observable<any>;
}

export interface ParameterProviderConfiguration {
    parameterGroupName: string;
    parameterProviderId: string;
    parameterProviderName: string;
    synchronized: boolean;
}

export interface ParameterProviderConfigurationEntity {
    id: string;
    component: ParameterProviderConfiguration;
}

export interface FetchComponentVersionsRequest {
    id: string;
    uri: string;
    revision: Revision;
    type: string;
    bundle: Bundle;
}

export interface OpenChangeComponentVersionDialogRequest {
    fetchRequest: FetchComponentVersionsRequest;
    componentVersions: DocumentedType[];
}

export interface ExternalControllerServiceReference {
    identifier: string;
    name: string;
}

export interface ClearBulletinsRequest {
    uri: string;
    fromTimestamp: string;
    componentId: string;
    componentType: ComponentType;
}

export interface ClearBulletinsResponse {
    componentId: string;
    bulletinsCleared: number;
    bulletins: BulletinEntity[];
    componentType: ComponentType;
}
