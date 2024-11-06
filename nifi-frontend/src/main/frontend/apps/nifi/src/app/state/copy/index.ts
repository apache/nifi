/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { ExternalControllerServiceReference } from '../shared';

export const copyFeatureKey = 'copy';

export interface CopyRequest {
    copyRequestEntity: CopyRequestEntity;
}
export interface CopyRequestContext extends CopyRequest {
    processGroupId: string;
}
export interface CopyResponseContext {
    copyResponse: CopyResponseEntity;
    processGroupId: string;
    pasteCount: number;
}
export interface CopyRequestEntity {
    processGroups?: string[];
    remoteProcessGroups?: string[];
    processors?: string[];
    inputPorts?: string[];
    outputPorts?: string[];
    connections?: string[];
    labels?: string[];
    funnels?: string[];
}
export interface CopyResponseEntity {
    id: string;
    processGroups?: any[];
    remoteProcessGroups?: any[];
    processors?: any[];
    inputPorts?: any[];
    outputPorts?: any[];
    connections?: any[];
    labels?: any[];
    funnels?: any[];
    externalControllerServiceReferences?: { [key: string]: ExternalControllerServiceReference };
    parameterContexts?: { [key: string]: any };
    parameterProviders?: { [key: string]: any };
}

export enum PasteRequestStrategy {
    CENTER_ON_CANVAS,
    OFFSET_FROM_ORIGINAL
}

export interface CopiedContentInfo {
    copyId: string;
    processGroupId: string;
    strategy: PasteRequestStrategy;
}

export interface CopyState {
    copiedContent: CopyResponseContext | null;
}
