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
import { HttpClient } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';
import {
    ConfigureReportingTaskRequest,
    CreateReportingTaskRequest,
    DeleteReportingTaskRequest,
    ReportingTaskEntity,
    StartReportingTaskRequest,
    StopReportingTaskRequest
} from '../state/reporting-tasks';

@Injectable({ providedIn: 'root' })
export class ReportingTaskService {
    private static readonly API: string = '../nifi-api';

    /**
     * The NiFi model contain the url for each component. That URL is an absolute URL. Angular CSRF handling
     * does not work on absolute URLs, so we need to strip off the proto for the request header to be added.
     *
     * https://stackoverflow.com/a/59586462
     *
     * @param url
     * @private
     */
    private stripProtocol(url: string): string {
        return this.nifiCommon.substringAfterFirst(url, ':');
    }

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getReportingTasks(): Observable<any> {
        return this.httpClient.get(`${ReportingTaskService.API}/flow/reporting-tasks`);
    }

    createReportingTask(createReportingTask: CreateReportingTaskRequest): Observable<any> {
        return this.httpClient.post(`${ReportingTaskService.API}/controller/reporting-tasks`, {
            revision: createReportingTask.revision,
            component: {
                bundle: createReportingTask.reportingTaskBundle,
                type: createReportingTask.reportingTaskType
            }
        });
    }

    deleteReportingTask(deleteReportingTask: DeleteReportingTaskRequest): Observable<any> {
        const entity: ReportingTaskEntity = deleteReportingTask.reportingTask;
        const revision: any = this.client.getRevision(entity);
        return this.httpClient.delete(this.stripProtocol(entity.uri), { params: revision });
    }

    startReportingTask(startReportingTask: StartReportingTaskRequest): Observable<any> {
        const entity: ReportingTaskEntity = startReportingTask.reportingTask;
        const revision: any = this.client.getRevision(entity);
        const payload: any = {
            revision,
            state: 'RUNNING'
        };
        return this.httpClient.put(`${this.stripProtocol(entity.uri)}/run-status`, payload);
    }

    stopReportingTask(stopReportingTask: StopReportingTaskRequest): Observable<any> {
        const entity: ReportingTaskEntity = stopReportingTask.reportingTask;
        const revision: any = this.client.getRevision(entity);
        const payload: any = {
            revision,
            state: 'STOPPED'
        };
        return this.httpClient.put(`${this.stripProtocol(entity.uri)}/run-status`, payload);
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
        return this.httpClient.put(this.stripProtocol(configureReportingTask.uri), configureReportingTask.payload);
    }
}
