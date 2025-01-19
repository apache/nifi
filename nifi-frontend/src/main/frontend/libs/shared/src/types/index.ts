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
