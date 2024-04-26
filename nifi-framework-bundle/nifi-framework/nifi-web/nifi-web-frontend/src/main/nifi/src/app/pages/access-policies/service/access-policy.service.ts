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

import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { Observable } from 'rxjs';
import { AccessPolicyEntity, ComponentResourceAction, ResourceAction } from '../state/shared';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { TenantEntity } from '../../../state/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class AccessPolicyService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {}

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
        let resource = `/${resourceAction.resource}`;
        if (resourceAction.resourceIdentifier) {
            resource += `/${resourceAction.resourceIdentifier}`;
        }

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
        const path: string[] = [resourceAction.action, resourceAction.resource];
        if (resourceAction.resourceIdentifier) {
            path.push(resourceAction.resourceIdentifier);
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

        return this.httpClient.put(this.nifiCommon.stripProtocol(accessPolicy.uri), payload);
    }

    deleteAccessPolicy(accessPolicy: AccessPolicyEntity): Observable<any> {
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(accessPolicy),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(this.nifiCommon.stripProtocol(accessPolicy.uri), { params });
    }

    getUsers(): Observable<any> {
        return this.httpClient.get(`${AccessPolicyService.API}/tenants/users`);
    }

    getUserGroups(): Observable<any> {
        return this.httpClient.get(`${AccessPolicyService.API}/tenants/user-groups`);
    }
}
