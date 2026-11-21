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
import { HttpClient } from '@angular/common/http';
import { defer, Observable } from 'rxjs';
import { ProcessGroupDrainRequestEntity, safeApiPath } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class ProcessGroupDrainService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-api';

    beginDrain(processGroupId: string): Observable<ProcessGroupDrainRequestEntity> {
        return defer(() =>
            this.httpClient.post<ProcessGroupDrainRequestEntity>(
                `${ProcessGroupDrainService.API}/process-groups/${safeApiPath(processGroupId)}/drain-requests`,
                {}
            )
        );
    }

    getDrain(processGroupId: string, drainRequestId: string): Observable<ProcessGroupDrainRequestEntity> {
        return defer(() =>
            this.httpClient.get<ProcessGroupDrainRequestEntity>(
                `${ProcessGroupDrainService.API}/process-groups/${safeApiPath(processGroupId)}/drain-requests/${safeApiPath(drainRequestId)}`
            )
        );
    }

    endDrain(processGroupId: string, drainRequestId: string): Observable<ProcessGroupDrainRequestEntity> {
        return defer(() =>
            this.httpClient.delete<ProcessGroupDrainRequestEntity>(
                `${ProcessGroupDrainService.API}/process-groups/${safeApiPath(processGroupId)}/drain-requests/${safeApiPath(drainRequestId)}`
            )
        );
    }
}
