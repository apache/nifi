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

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '@nifi/shared';
import {
    ConfigureReportingTaskRequest,
    CreateReportingTaskRequest,
    DeleteReportingTaskRequest,
    ReportingTaskEntity,
    StartReportingTaskRequest,
    StopReportingTaskRequest
} from '../state/reporting-tasks';
import { PropertyDescriptorRetriever } from '../../../state/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class ReportingTaskService implements PropertyDescriptorRetriever {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {}

    getReportingTasks(): Observable<any> {
        return this.httpClient.get(`${ReportingTaskService.API}/flow/reporting-tasks`);
    }

    getReportingTask(id: string): Observable<any> {
        return this.httpClient.get(`${ReportingTaskService.API}/reporting-tasks/${id}`);
    }

    createReportingTask(createReportingTask: CreateReportingTaskRequest): Observable<any> {
        return this.httpClient.post(`${ReportingTaskService.API}/controller/reporting-tasks`, {
            revision: createReportingTask.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                bundle: createReportingTask.reportingTaskBundle,
                type: createReportingTask.reportingTaskType
            }
        });
    }

    deleteReportingTask(deleteReportingTask: DeleteReportingTaskRequest): Observable<any> {
        const entity: ReportingTaskEntity = deleteReportingTask.reportingTask;
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(entity),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(this.nifiCommon.stripProtocol(entity.uri), { params });
    }

    startReportingTask(startReportingTask: StartReportingTaskRequest): Observable<any> {
        const entity: ReportingTaskEntity = startReportingTask.reportingTask;
        const revision: any = this.client.getRevision(entity);
        const payload: any = {
            revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            state: 'RUNNING'
        };
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(entity.uri)}/run-status`, payload);
    }

    stopReportingTask(stopReportingTask: StopReportingTaskRequest): Observable<any> {
        const entity: ReportingTaskEntity = stopReportingTask.reportingTask;
        const revision: any = this.client.getRevision(entity);
        const payload: any = {
            revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            state: 'STOPPED'
        };
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(entity.uri)}/run-status`, payload);
    }

    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<any> {
        const params: any = {
            propertyName,
            sensitive
        };
        return this.httpClient.get(`${ReportingTaskService.API}/reporting-tasks/${id}/descriptors`, {
            params
        });
    }

    updateReportingTask(configureReportingTask: ConfigureReportingTaskRequest): Observable<any> {
        return this.httpClient.put(
            this.nifiCommon.stripProtocol(configureReportingTask.uri),
            configureReportingTask.payload
        );
    }
}
