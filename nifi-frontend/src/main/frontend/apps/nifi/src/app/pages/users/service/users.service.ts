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
import { UserEntity, UserGroupEntity } from '../../../state/shared';
import {
    CreateUserGroupRequest,
    CreateUserRequest,
    UpdateUserGroupRequest,
    UpdateUserRequest
} from '../state/user-listing';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class UsersService {
    private httpClient = inject(HttpClient);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    getUsers(): Observable<any> {
        return this.httpClient.get(`${UsersService.API}/tenants/users`);
    }

    getUserGroups(): Observable<any> {
        return this.httpClient.get(`${UsersService.API}/tenants/user-groups`);
    }

    createUser(request: CreateUserRequest): Observable<any> {
        const payload: any = {
            revision: request.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: request.userPayload
        };
        return this.httpClient.post(`${UsersService.API}/tenants/users`, payload);
    }

    createUserGroup(request: CreateUserGroupRequest): Observable<any> {
        const payload: any = {
            revision: request.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: request.userGroupPayload
        };
        return this.httpClient.post(`${UsersService.API}/tenants/user-groups`, payload);
    }

    updateUser(request: UpdateUserRequest): Observable<any> {
        const payload: any = {
            revision: request.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: request.id,
                ...request.userPayload
            }
        };
        return this.httpClient.put(`${UsersService.API}/tenants/users/${request.id}`, payload);
    }

    updateUserGroup(request: UpdateUserGroupRequest): Observable<any> {
        const payload: any = {
            revision: request.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: request.id,
                ...request.userGroupPayload
            }
        };
        return this.httpClient.put(`${UsersService.API}/tenants/user-groups/${request.id}`, payload);
    }

    deleteUser(user: UserEntity): Observable<any> {
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(user),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(`${UsersService.API}/tenants/users/${user.id}`, { params });
    }

    deleteUserGroup(userGroup: UserGroupEntity): Observable<any> {
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(userGroup),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(`${UsersService.API}/tenants/user-groups/${userGroup.id}`, { params });
    }
}
