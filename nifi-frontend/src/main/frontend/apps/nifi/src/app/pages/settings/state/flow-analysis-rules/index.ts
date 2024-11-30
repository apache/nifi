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

import { Bundle, ComponentHistory, DocumentedType } from '../../../../state/shared';
import { BulletinEntity, Permissions, Revision } from '@nifi/shared';

export const flowAnalysisRulesFeatureKey = 'flowAnalysisRules';

export interface CreateFlowAnalysisRuleDialogRequest {
    flowAnalysisRuleTypes: DocumentedType[];
}

export interface LoadFlowAnalysisRulesResponse {
    flowAnalysisRules: FlowAnalysisRuleEntity[];
    loadedTimestamp: string;
}

export interface CreateFlowAnalysisRuleRequest {
    flowAnalysisRuleType: string;
    flowAnalysisRuleBundle: Bundle;
    revision: Revision;
}

export interface CreateFlowAnalysisRuleSuccess {
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface ConfigureFlowAnalysisRuleRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface ConfigureFlowAnalysisRuleSuccess {
    id: string;
    flowAnalysisRule: FlowAnalysisRuleEntity;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface UpdateFlowAnalysisRuleRequest {
    payload: any;
    postUpdateNavigation?: string[];
    postUpdateNavigationBoundary?: string[];
}

export interface EnableFlowAnalysisRuleSuccess {
    id: string;
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface DisableFlowAnalysisRuleSuccess {
    id: string;
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface EnableFlowAnalysisRuleRequest {
    id: string;
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface DisableFlowAnalysisRuleRequest {
    id: string;
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface ConfigureFlowAnalysisRuleRequest {
    id: string;
    uri: string;
    payload: any;
    postUpdateNavigation?: string[];
}

export interface EditFlowAnalysisRuleDialogRequest {
    id: string;
    flowAnalysisRule: FlowAnalysisRuleEntity;
    history?: ComponentHistory;
}

export interface DeleteFlowAnalysisRuleRequest {
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface DeleteFlowAnalysisRuleSuccess {
    flowAnalysisRule: FlowAnalysisRuleEntity;
}

export interface SelectFlowAnalysisRuleRequest {
    id: string;
}

export interface FlowAnalysisRuleEntity {
    permissions: Permissions;
    operatePermissions?: Permissions;
    revision: Revision;
    bulletins: BulletinEntity[];
    id: string;
    uri: string;
    status: any;
    component: any;
}

export interface FlowAnalysisRulesState {
    flowAnalysisRules: FlowAnalysisRuleEntity[];
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
