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

import { Injectable, inject } from '@angular/core';
import { defer, EMPTY, Observable } from 'rxjs';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import {
    ControllerServiceCreator,
    CreateControllerServiceRequest,
    PropertyDescriptorRetriever
} from '../../../state/shared';
import { ConfigureControllerServiceRequest, DeleteControllerServiceRequest } from '../../../state/flow-shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ControllerServiceEntity, safeApiPath } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class ControllerServiceService implements ControllerServiceCreator, PropertyDescriptorRetriever {
    private httpClient = inject(HttpClient);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    getControllerServices(processGroupId: string): Observable<any> {
        const uiOnly: any = { uiOnly: true };
        return defer(() =>
            this.httpClient.get(
                `${ControllerServiceService.API}/flow/process-groups/${safeApiPath(processGroupId)}/controller-services`,
                {
                    params: uiOnly
                }
            )
        );
    }

    getFlow(processGroupId: string): Observable<any> {
        return defer(() =>
            this.httpClient.get(`${ControllerServiceService.API}/flow/process-groups/${safeApiPath(processGroupId)}`)
        );
    }

    getControllerService(id: string): Observable<ControllerServiceEntity> {
        const uiOnly: any = { uiOnly: true };
        return defer(() =>
            this.httpClient.get<ControllerServiceEntity>(
                `${ControllerServiceService.API}/controller-services/${safeApiPath(id)}`,
                { params: uiOnly }
            )
        );
    }

    createControllerService(createControllerService: CreateControllerServiceRequest): Observable<any> {
        if (createControllerService.processGroupId) {
            const processGroupId: string = createControllerService.processGroupId;
            return defer(() =>
                this.httpClient.post(
                    `${ControllerServiceService.API}/process-groups/${safeApiPath(processGroupId)}/controller-services`,
                    {
                        revision: createControllerService.revision,
                        disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                        component: {
                            bundle: createControllerService.controllerServiceBundle,
                            type: createControllerService.controllerServiceType
                        }
                    }
                )
            );
        }
        return EMPTY;
    }

    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<any> {
        const params: any = {
            propertyName,
            sensitive
        };
        return defer(() =>
            this.httpClient.get(`${ControllerServiceService.API}/controller-services/${safeApiPath(id)}/descriptors`, {
                params
            })
        );
    }

    updateControllerService(configureControllerService: ConfigureControllerServiceRequest): Observable<any> {
        return defer(() =>
            this.httpClient.put(
                `${ControllerServiceService.API}/controller-services/${safeApiPath(configureControllerService.id)}`,
                configureControllerService.payload
            )
        );
    }

    clearBulletins(request: { id: string; fromTimestamp: string }): Observable<any> {
        const payload = {
            fromTimestamp: request.fromTimestamp
        };
        return defer(() =>
            this.httpClient.post(
                `${ControllerServiceService.API}/controller-services/${safeApiPath(request.id)}/bulletins/clear-requests`,
                payload
            )
        );
    }

    deleteControllerService(deleteControllerService: DeleteControllerServiceRequest): Observable<any> {
        const entity: ControllerServiceEntity = deleteControllerService.controllerService;
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(entity),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return defer(() =>
            this.httpClient.delete(`${ControllerServiceService.API}/controller-services/${safeApiPath(entity.id)}`, {
                params
            })
        );
    }
}
