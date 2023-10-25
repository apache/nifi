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

export interface YesNoDialogRequest {
    title: string;
    message: string;
}

export interface NewPropertyDialogRequest {
    allowsSensitive: boolean;
}

export interface NewPropertyDialogResponse {
    name: string;
    sensitive: boolean;
}

export interface ParameterDetails {
    name: string;
    description: string;
    sensitive: boolean;
    value: string | null;
    valueRemoved?: boolean;
}

export interface EditParameterRequest {
    parameter?: ParameterDetails;
}

export interface EditParameterResponse {
    parameter: ParameterDetails;
}

export interface CreateControllerServiceRequest {
    controllerServiceTypes: DocumentedType[];
}

export interface EditControllerServiceRequest {
    id: string;
    controllerService: ControllerServiceEntity;
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
    clientId: string;
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
    uri: string;
    status: any;
    component: any;
}

export interface DocumentedType {
    bundle: Bundle;
    description: string;
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

export interface PropertyDependency {
    propertyName: string;
    dependentValues: string[];
}

export interface PropertyDescriptor {
    name: string;
    displayName: string;
    description: string;
    defaultValue: string;
    allowableValues: AllowableValueEntity[];
    required: boolean;
    sensitive: boolean;
    dynamic: boolean;
    supportsEl: boolean;
    expressionLanguageScope: string;
    identifiesControllerService: string;
    identifiesControllerServiceBundle: Bundle;
    dependencies: PropertyDependency[];
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
