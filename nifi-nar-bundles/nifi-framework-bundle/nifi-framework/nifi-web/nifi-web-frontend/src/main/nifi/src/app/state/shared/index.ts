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

export interface OkDialogRequest {
    title: string;
    message: string;
}

export interface CancelDialogRequest {
    title: string;
    message: string;
}

export interface YesNoDialogRequest {
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
}

export interface EditParameterResponse {
    parameter: Parameter;
}

export interface CreateControllerServiceRequest {
    controllerServiceTypes: DocumentedType[];
}

export interface EditControllerServiceDialogRequest {
    id: string;
    controllerService: ControllerServiceEntity;
}

export interface UpdateControllerServiceRequest {
    payload: any;
    postUpdateNavigation?: string[];
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
    attributes: Attribute[];
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

export interface TextTipInput {
    text: string;
}

export interface UnorderedListTipInput {
    items: string[];
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

export interface PropertyTipInput {
    descriptor: PropertyDescriptor;
}

export interface ParameterTipInput {
    parameter: Parameter;
}

export interface ElFunctionTipInput {
    elFunction: ElFunction;
}

export interface PropertyHintTipInput {
    supportsEl: boolean;
    supportsParameters: boolean;
}

export interface RestrictionsTipInput {
    usageRestriction: string;
    explicitRestrictions: ExplicitRestriction[];
}

export interface Permissions {
    canRead: boolean;
    canWrite: boolean;
}

export interface ExplicitRestriction {
    requiredPermission: RequiredPermission;
    explanation: string;
}

export interface RequiredPermission {
    id: string;
    label: string;
}

export interface Revision {
    version: number;
    clientId?: string;
}

export interface BulletinEntity {
    canRead: boolean;
    id: number;
    sourceId: string;
    groupId: string;
    timestamp: string;
    nodeAddress?: string;
    bulletin: {
        id: number;
        sourceId: string;
        groupId: string;
        category: string;
        level: string;
        message: string;
        sourceName: string;
        timestamp: string;
        nodeAddress?: string;
    };
}

export interface ParameterEntity {
    canWrite?: boolean;
    parameter: Parameter;
}

export interface Parameter {
    name: string;
    description: string;
    sensitive: boolean;
    value: string | null;
    valueRemoved?: boolean;
    provided?: boolean;
    referencingComponents?: AffectedComponentEntity[];
    parameterContext?: ParameterContextReferenceEntity;
    inherited?: boolean;
}

export interface ParameterContextReferenceEntity {
    permissions: Permissions;
    id: string;
    component?: ParameterContextReference;
}

export interface ParameterContextReference {
    id: string;
    name: string;
}

export interface AffectedComponentEntity {
    permissions: Permissions;
    id: string;
    revision: Revision;
    bulletins: BulletinEntity[];
    component: AffectedComponent;
    processGroup: ProcessGroupName;
    referenceType: string;
}

export interface AffectedComponent {
    processGroupId: string;
    id: string;
    referenceType: string;
    name: string;
    state: string;
    activeThreadCount?: number;
    validationErrors: string[];
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
}

export interface ParameterContextUpdateRequestEntity {
    parameterContextRevision: Revision;
    request: ParameterContextUpdateRequest;
}

export interface ElFunction {
    name: string;
    description: string;
    args: { [key: string]: string };
    subject?: string;
    returnType: string;
}

export interface ProcessGroupName {
    id: string;
    name: string;
}

export enum ComponentType {
    Processor = 'Processor',
    ProcessGroup = 'ProcessGroup',
    RemoteProcessGroup = 'RemoteProcessGroup',
    InputPort = 'InputPort',
    OutputPort = 'OutputPort',
    Label = 'Label',
    Funnel = 'Funnel',
    Connection = 'Connection'
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

export interface Bundle {
    artifact: string;
    group: string;
    version: string;
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

export interface SelectOption {
    text: string;
    value: string | null;
    description?: string;
    disabled?: boolean;
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
