/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { AccessPolicySummary, TenantEntity } from '../../../../state/shared';
import { Revision, Permissions } from '@nifi/shared';

export enum PolicyStatus {
    Found = 'Found',
    Inherited = 'Inherited',
    NotFound = 'NotFound',
    Forbidden = 'Forbidden'
}

export enum Action {
    Read = 'read',
    Write = 'write'
}

export interface ResourceAction {
    resource: string;
    resourceIdentifier?: string;
    action: Action;
}

export interface ComponentResourceAction extends ResourceAction {
    resourceIdentifier: string;
    policy: string;
}

export interface AccessPolicyEntity {
    id: string;
    component: AccessPolicy;
    revision: Revision;
    uri: string;
    permissions: Permissions;
    generated: string;
}

export interface AccessPolicy extends AccessPolicySummary {
    users: TenantEntity[];
    userGroups: TenantEntity[];
}
