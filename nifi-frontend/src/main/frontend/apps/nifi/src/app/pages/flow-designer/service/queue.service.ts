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
import { NiFiCommon } from '@nifi/shared';
import { DropRequest, SubmitEmptyQueueRequest, SubmitEmptyQueuesRequest } from '../state/queue';

@Injectable({ providedIn: 'root' })
export class QueueService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private nifiCommon: NiFiCommon
    ) {}

    submitEmptyQueueRequest(emptyQueueRequest: SubmitEmptyQueueRequest): Observable<any> {
        return this.httpClient.post(
            `${QueueService.API}/flowfile-queues/${emptyQueueRequest.connectionId}/drop-requests`,
            {}
        );
    }

    submitEmptyQueuesRequest(emptyQueuesRequest: SubmitEmptyQueuesRequest): Observable<any> {
        return this.httpClient.post(
            `${QueueService.API}/process-groups/${emptyQueuesRequest.processGroupId}/empty-all-connections-requests`,
            {}
        );
    }

    pollEmptyQueueRequest(dropRequest: DropRequest): Observable<any> {
        return this.httpClient.get(this.nifiCommon.stripProtocol(dropRequest.uri));
    }

    deleteEmptyQueueRequest(dropRequest: DropRequest): Observable<any> {
        return this.httpClient.delete(this.nifiCommon.stripProtocol(dropRequest.uri));
    }
}
