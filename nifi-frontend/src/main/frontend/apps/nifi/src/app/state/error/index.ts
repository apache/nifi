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

export const errorFeatureKey = 'error';

export interface ErrorDetail {
    title: string;
    message: string;
}

export enum ErrorContextKey {
    ACCESS_POLICIES = 'access-policies',
    QUEUE = 'queue',
    CLUSTER = 'cluster',
    PROVENANCE = 'provenance',
    COMPONENT_STATE = 'component-state',
    STATUS_HISTORY = 'status-history',
    SYSTEM_DIAGNOSTICS = 'system-diagnostics',
    CONTROLLER_SERVICES = 'controller-services',
    FLOW = 'flow',
    MANAGE_REMOTE_PORTS = 'manage-remote-ports',
    PARAMETER_CONTEXTS = 'parameter-contexts',
    PARAMETER_PROVIDERS = 'parameter-providers',
    REGISTRY_CLIENTS = 'registry-clients',
    REPORTING_TASKS = 'report-tasks',
    USERS = 'users',
    PROCESS_GROUP = 'process-group',
    REMOTE_PROCESS_GROUP = 'remote-process-group',
    PROCESSOR = 'processor',
    CONNECTION = 'connection',
    PORT = 'port',
    REGISTRY_IMPORT = 'registry-import',
    LABEL = 'label',
    FLOW_VERSION = 'flow-version',
    FUNNEL = 'funnel',
    LOCAL_EXTENSIONS = 'local-extensions',
    LINEAGE = 'lineage',
    FLOW_ANALYSIS_RULES = 'flow-analysis-rules'
}

export interface ErrorContext {
    context: ErrorContextKey;
    errors: string[];
}

export interface BannerErrors {
    // key should be the ErrorContextKey of the banner error
    [key: string]: string[];
}

export interface ErrorState {
    bannerErrors: BannerErrors;
    fullScreenError: ErrorDetail | null;
    routedToFullScreenError: boolean;
}
