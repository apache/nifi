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

import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { Observable } from 'rxjs';
import { AccessPolicyEntity, ComponentResourceAction, ResourceAction } from '../state/shared';
import { TenantEntity } from '../../../state/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class AccessPolicyService {
    private httpClient = inject(HttpClient);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    /**
     * Transforms resourceAction for connector data/provenance policies.
     * When the UI uses 'connectors' as resource with 'data' or 'provenance-data' as resourceIdentifier,
     * this transforms it to the API-expected format where 'data' or 'provenance-data' becomes the
     * resource prefix and 'connectors' becomes the suffix.
     *
     * Examples:
     * - { resource: 'connectors', resourceIdentifier: 'data' } -> { resource: 'data/connectors', resourceIdentifier: undefined }
     * - { resource: 'connectors', resourceIdentifier: 'provenance-data' } -> { resource: 'provenance-data/connectors', resourceIdentifier: undefined }
     */
    private transformConnectorPolicyResource(resourceAction: ResourceAction): ResourceAction {
        if (
            resourceAction.resource === 'connectors' &&
            (resourceAction.resourceIdentifier === 'data' || resourceAction.resourceIdentifier === 'provenance-data')
        ) {
            return {
                ...resourceAction,
                resource: `${resourceAction.resourceIdentifier}/connectors`,
                resourceIdentifier: undefined
            };
        }
        return resourceAction;
    }

    /**
     * Builds the API resource path from a ResourceAction.
     * Handles connector data/provenance policy transformations.
     */
    buildResourcePath(resourceAction: ResourceAction): string {
        const transformed = this.transformConnectorPolicyResource(resourceAction);
        let resource = `/${transformed.resource}`;
        if (transformed.resourceIdentifier) {
            resource += `/${transformed.resourceIdentifier}`;
        }
        return resource;
    }

    createAccessPolicy(
        resourceAction: ResourceAction,
        {
            userGroups = [],
            users = []
        }: {
            userGroups?: TenantEntity[];
            users?: TenantEntity[];
        } = {}
    ): Observable<any> {
        const resource = this.buildResourcePath(resourceAction);

        const payload: unknown = {
            revision: {
                version: 0,
                clientId: this.client.getClientId()
            },
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                action: resourceAction.action,
                resource,
                userGroups,
                users
            }
        };

        return this.httpClient.post(`${AccessPolicyService.API}/policies`, payload);
    }

    getAccessPolicy(resourceAction: ResourceAction): Observable<any> {
        const transformed = this.transformConnectorPolicyResource(resourceAction);
        const path: string[] = [transformed.action, transformed.resource];
        if (transformed.resourceIdentifier) {
            path.push(transformed.resourceIdentifier);
        }
        return this.httpClient.get(`${AccessPolicyService.API}/policies/${path.join('/')}`);
    }

    getPolicyComponent(resourceAction: ComponentResourceAction): Observable<any> {
        return this.httpClient.get(
            `${AccessPolicyService.API}/${resourceAction.resource}/${resourceAction.resourceIdentifier}`
        );
    }

    updateAccessPolicy(accessPolicy: AccessPolicyEntity, users: TenantEntity[], userGroups: TenantEntity[]) {
        const payload: unknown = {
            revision: this.client.getRevision(accessPolicy),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: accessPolicy.id,
                userGroups,
                users
            }
        };

        return this.httpClient.put(`${AccessPolicyService.API}/policies/${accessPolicy.id}`, payload);
    }

    deleteAccessPolicy(accessPolicy: AccessPolicyEntity): Observable<any> {
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(accessPolicy),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(`${AccessPolicyService.API}/policies/${accessPolicy.id}`, { params });
    }

    getUsers(): Observable<any> {
        return this.httpClient.get(`${AccessPolicyService.API}/tenants/users`);
    }

    getUserGroups(): Observable<any> {
        return this.httpClient.get(`${AccessPolicyService.API}/tenants/user-groups`);
    }
}
