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

import { MatDialogConfig } from '@angular/material/dialog';
import { filter, Observable } from 'rxjs';

export const SMALL_DIALOG: MatDialogConfig = {
    maxWidth: '24rem',
    minWidth: 320,
    disableClose: true,
    closeOnNavigation: false
};
export const MEDIUM_DIALOG: MatDialogConfig = {
    maxWidth: 470,
    minWidth: 470,
    disableClose: true,
    closeOnNavigation: false
};
export const LARGE_DIALOG: MatDialogConfig = {
    maxWidth: 760,
    minWidth: 760,
    disableClose: true,
    closeOnNavigation: false
};
export const XL_DIALOG: MatDialogConfig = {
    maxWidth: 1024,
    minWidth: 1024,
    disableClose: true,
    closeOnNavigation: false
};

export enum ComponentType {
    Processor = 'Processor',
    ProcessGroup = 'ProcessGroup',
    RemoteProcessGroup = 'RemoteProcessGroup',
    InputPort = 'InputPort',
    OutputPort = 'OutputPort',
    Label = 'Label',
    Funnel = 'Funnel',
    Connection = 'Connection',
    ControllerService = 'ControllerService',
    ReportingTask = 'ReportingTask',
    FlowAnalysisRule = 'FlowAnalysisRule',
    ParameterProvider = 'ParameterProvider',
    FlowRegistryClient = 'FlowRegistryClient',
    Connector = 'Connector',
    Flow = 'Flow'
}

export interface SelectGroup {
    text: string;
    options: SelectOption[];
}

export interface SelectOption {
    text: string;
    value: string | null;
    description?: string;
    disabled?: boolean;
}

export interface MapTableEntry {
    name: string;
    value: string | null;
}

export interface MapTableItem {
    entry: MapTableEntry;
    id: number;
    triggerEdit: boolean;
    deleted: boolean;
    dirty: boolean;
    added: boolean;
}

export interface MapTableEntryData {
    existingEntries: string[];
    entryTypeLabel?: string;
}

export interface PropertyHintTipInput {
    supportsEl: boolean;
    showParameters: boolean;
    supportsParameters: boolean;
    hasParameterContext: boolean;
}

export interface ElFunction {
    name: string;
    description: string;
    args: { [key: string]: string };
    subject?: string;
    returnType: string;
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
    referencedAssets?: ReferencedAsset[];
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

export interface ParameterContextReferenceEntity {
    permissions: Permissions;
    id: string;
    component?: ParameterContextReference;
    bulletins?: BulletinEntity[];
}

export interface ParameterContextReference {
    id: string;
    name: string;
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

export interface ReferencedAsset {
    id: string;
    name: string;
}

export interface Permissions {
    canRead: boolean;
    canWrite: boolean;
}

export interface Revision {
    version: number;
    clientId?: string;
    lastModifier?: string;
}

export interface BulletinEntity {
    canRead: boolean;
    id: number;
    sourceId: string;
    groupId: string;
    timestamp: string;
    timestampIso: string;
    nodeAddress?: string;
    bulletin: {
        id: number;
        sourceId: string;
        groupId: string;
        category: string;
        level: string;
        message: string;
        stackTrace?: string;
        sourceName: string;
        timestamp: string;
        timestampIso: string;
        nodeAddress?: string;
        sourceType: string;
    };
}

export interface ProcessGroupName {
    id: string;
    name: string;
}

export interface ParameterTipInput {
    parameter: Parameter;
}

export interface ElFunctionTipInput {
    elFunction: ElFunction;
}

export interface YesNoDialogRequest {
    title: string;
    message: string;
}

export function isDefinedAndNotNull<T>() {
    return (source$: Observable<null | undefined | T>) =>
        source$.pipe(
            filter((input: null | undefined | T): input is T => {
                return input !== null && typeof input !== 'undefined';
            })
        );
}

export interface Bundle {
    artifact: string;
    group: string;
    version: string;
}

export type ConnectorActionName =
    | 'START'
    | 'STOP'
    | 'CONFIGURE'
    | 'DISCARD_WORKING_CONFIGURATION'
    | 'PURGE_FLOWFILES'
    | 'DRAIN_FLOWFILES'
    | 'CANCEL_DRAIN_FLOWFILES'
    | 'APPLY_UPDATES'
    | 'DELETE';

export interface ConnectorAction {
    name: ConnectorActionName;
    description: string;
    allowed: boolean;
    reasonNotAllowed?: string;
}

export enum ConnectorState {
    STARTING = 'STARTING',
    RUNNING = 'RUNNING',
    STOPPING = 'STOPPING',
    STOPPED = 'STOPPED',
    DRAINING = 'DRAINING',
    DISABLED = 'DISABLED',
    PREPARING_FOR_UPDATE = 'PREPARING_FOR_UPDATE',
    UPDATING = 'UPDATING',
    UPDATE_FAILED = 'UPDATE_FAILED'
}

export interface ConnectorComponent {
    id: string;
    name: string;
    type: string;
    state: string;
    bundle: Bundle;
    activeConfiguration?: ConnectorConfiguration;
    workingConfiguration?: ConnectorConfiguration;
    validationErrors?: string[];
    validationStatus?: string;
    multipleVersionsAvailable?: boolean;
    configurationUrl?: string;
    detailsUrl?: string;
    availableActions: ConnectorAction[];
    managedProcessGroupId: string;
}

export interface ConnectorStatusSnapshot {
    id: string;
    groupId: string;
    name: string;
    type: string;
    runStatus: string;
    flowFilesSent: number;
    bytesSent: number;
    flowFilesReceived: number;
    bytesReceived: number;
    bytesRead: number;
    bytesWritten: number;
    sent: string;
    received: string;
    read: string;
    written: string;
    flowFilesQueued: number;
    bytesQueued: number;
    queued: string;
    queuedCount: string;
    queuedSize: string;
    activeThreadCount: number;
    idle?: boolean;
    idleDurationMillis?: number;
    idleDuration?: string;
}

export interface NodeConnectorStatusSnapshot {
    nodeId: string;
    address: string;
    apiPort: number;
    statusSnapshot: ConnectorStatusSnapshot;
}

export interface ConnectorStatus {
    id: string;
    groupId: string;
    name: string;
    type: string;
    runStatus: string;
    validationStatus: string;
    statsLastRefreshed: string;
    aggregateSnapshot: ConnectorStatusSnapshot;
    nodeSnapshots?: NodeConnectorStatusSnapshot[];
}

export interface ConnectorEntity {
    permissions: Permissions;
    operatePermissions?: Permissions;
    revision: Revision;
    bulletins: BulletinEntity[];
    id: string;
    uri: string;
    status: ConnectorStatus;
    component: ConnectorComponent;
}

// ========================================================================================
// Connector Configuration Wizard Types
// ========================================================================================

/** Variant for status-style banners and inline messaging */
export type StatusVariant = 'neutral' | 'active' | 'critical' | 'caution' | 'success' | 'info';

export type PropertyType =
    | 'STRING'
    | 'INTEGER'
    | 'BOOLEAN'
    | 'DOUBLE'
    | 'FLOAT'
    | 'STRING_LIST'
    | 'SECRET'
    | 'ASSET'
    | 'ASSET_LIST';

export interface ConnectorPropertyDescriptor {
    name: string;
    description?: string;
    type: PropertyType;
    required: boolean;
    defaultValue?: any;
    allowableValues?: AllowableValue[];
    allowableValuesFetchable?: boolean;
    dependencies?: ConnectorPropertyDependency[];
}

export type ConnectorValueType = 'STRING_LITERAL' | 'ASSET_REFERENCE' | 'SECRET_REFERENCE';

export interface AssetReference {
    id: string;
    name?: string;
    missingContent?: boolean;
}

export interface AssetInfo {
    id: string;
    name: string;
    digest?: string;
    missingContent?: boolean;
}

/** Upload progress information for connector asset uploads */
export interface UploadProgressInfo {
    filename: string;
    percentComplete: number;
    status: 'active' | 'complete' | 'error';
    error?: string;
}

export interface ConnectorValueReference {
    value?: string | null;
    valueType: ConnectorValueType;
    assetReferences?: AssetReference[];
    secretName?: string;
    secretProviderId?: string;
    secretProviderName?: string;
    fullyQualifiedSecretName?: string;
}

export interface PropertyGroupConfiguration {
    propertyGroupName: string;
    propertyGroupDescription?: string;
    propertyDescriptors: { [propertyName: string]: ConnectorPropertyDescriptor };
    propertyValues: { [propertyName: string]: ConnectorValueReference };
}

export interface Secret {
    name: string;
    fullyQualifiedName: string;
    providerId: string;
    providerName: string;
    groupName: string;
    description?: string;
}

export interface SecretsEntity {
    secrets: Secret[];
}

export interface AllowableValue {
    allowableValue: {
        displayName: string;
        value: string;
    };
    canRead: boolean;
}

export interface PropertyAllowableValuesState {
    loading: boolean;
    error: string | null;
    values: AllowableValue[] | null;
}

export interface StepDocumentationEntity {
    stepDocumentation: string;
}

export interface StepDocumentationState {
    loading: boolean;
    error: string | null;
    stepDocumentation: string | null;
    loaded?: boolean;
}

export interface ConnectorConfiguration {
    configurationStepConfigurations: ConfigurationStepConfiguration[];
}

export interface ConfigurationStepDependency {
    stepName: string;
    propertyName: string;
    dependentValues?: string[];
}

export interface ConfigurationStepConfiguration {
    configurationStepName: string;
    configurationStepDescription?: string;
    documented?: boolean;
    propertyGroupConfigurations: PropertyGroupConfiguration[];
    dependencies: ConfigurationStepDependency[];
}

export interface ConfigurationStepNamesEntity {
    configurationStepNames: string[];
}

export interface ConfigurationStepEntity {
    configurationStep: ConfigurationStepConfiguration;
    parentConnectorId: string;
    parentConnectorRevision: Revision;
    disconnectedNodeAcknowledged?: boolean;
}

export interface ConfigVerificationResult {
    outcome: 'SUCCESSFUL' | 'FAILED' | 'SKIPPED';
    verificationStepName: string;
    subject?: string;
    explanation: string;
}

export interface ConnectorConfigStepVerificationRequest {
    requestId: string;
    uri: string;
    connectorId: string;
    configurationStepName: string;
    configurationStep: ConfigurationStepConfiguration;
    complete: boolean;
    percentCompleted: number;
    state: string;
    failureReason: string | null;
    results: ConfigVerificationResult[] | null;
    submissionTime?: string;
    lastUpdated?: string;
}

export interface ConnectorConfigStepVerificationRequestEntity {
    request: ConnectorConfigStepVerificationRequest;
}

export interface ConnectorPropertyAllowableValuesEntity {
    allowableValues: AllowableValue[];
}

export interface ConnectorPropertyDependency {
    propertyName: string;
    dependentValues?: string[];
}

export enum UploadContextKey {
    CUSTOM_NAR = 'custom-nar',
    ASSET = 'asset',
    CONNECTOR_ASSET = 'connector-asset'
}

export enum UploadStatus {
    ACTIVE = 'active',
    COMPLETE = 'complete',
    ERROR = 'ERROR'
}

export interface UploadRequest {
    context: UploadContextKey;
    url: string;
    file: File;
    monitorGlobally: boolean;
    metadata?: Record<string, any>;
}

export interface UploadSubmission {
    id: number;
    request: UploadRequest;
}

export interface UploadProgress {
    id: number;
    context: UploadContextKey;
    filename: string;
    percentComplete: number;
    status: UploadStatus;
    error: string | null;
    monitorGlobally: boolean;
    completedResponseObject?: any;
    metadata?: Record<string, any>;
}

export function buildSecretKey(
    providerId: string | undefined,
    providerName: string | undefined,
    fullyQualifiedName: string | undefined
): string {
    return [providerId ?? '', providerName ?? '', fullyQualifiedName ?? ''].join('::');
}

export function parseSecretKey(key: string): {
    providerId: string;
    providerName: string;
    fullyQualifiedName: string;
} {
    const parts = key.split('::');
    return {
        providerId: parts[0] ?? '',
        providerName: parts[1] ?? '',
        fullyQualifiedName: parts.slice(2).join('::')
    };
}

/**
 * Union of all primitive form-control value shapes used by the connector wizard.
 * Covers STRING, INTEGER, DOUBLE, FLOAT, SECRET (string), BOOLEAN (boolean),
 * STRING_LIST/ASSET_LIST (string[]), ASSET/ASSET_LIST from API (AssetReference / AssetReference[]),
 * and cleared/unset values (null).
 */
export type ConnectorPropertyFormValue = string | boolean | string[] | AssetReference | AssetReference[] | null;

export * from './connector-message.types';
