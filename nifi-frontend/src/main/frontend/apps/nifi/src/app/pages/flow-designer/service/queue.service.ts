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
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import {
    EmptyQueueRequest,
    EmptyQueuesRequest,
    SubmitEmptyQueueRequest,
    SubmitEmptyQueuesRequest
} from '../state/queue';

@Injectable({ providedIn: 'root' })
export class QueueService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-api';

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

    pollEmptyQueueRequest(request: EmptyQueueRequest): Observable<any> {
        return this.httpClient.get(
            `${QueueService.API}/flowfile-queues/${request.connectionId}/drop-requests/${request.dropRequestId}`
        );
    }

    deleteEmptyQueueRequest(request: EmptyQueueRequest): Observable<any> {
        return this.httpClient.delete(
            `${QueueService.API}/flowfile-queues/${request.connectionId}/drop-requests/${request.dropRequestId}`
        );
    }

    pollEmptyQueuesRequest(request: EmptyQueuesRequest): Observable<any> {
        return this.httpClient.get(
            `${QueueService.API}/process-groups/${request.processGroupId}/empty-all-connections-requests/${request.dropRequestId}`
        );
    }

    deleteEmptyQueuesRequest(request: EmptyQueuesRequest): Observable<any> {
        return this.httpClient.delete(
            `${QueueService.API}/process-groups/${request.processGroupId}/empty-all-connections-requests/${request.dropRequestId}`
        );
    }
}
