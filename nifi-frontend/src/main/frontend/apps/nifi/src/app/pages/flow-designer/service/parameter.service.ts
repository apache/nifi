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
import { HttpClient, HttpParams } from '@angular/common/http';
import { SubmitParameterContextUpdate } from '../../../state/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class ParameterService {
    private httpClient = inject(HttpClient);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    getParameterContext(id: string, includeInheritedParameters: boolean): Observable<any> {
        return this.httpClient.get(`${ParameterService.API}/parameter-contexts/${id}`, {
            params: {
                includeInheritedParameters
            }
        });
    }

    submitParameterContextUpdate(configureParameterContext: SubmitParameterContextUpdate): Observable<any> {
        return this.httpClient.post(
            `${ParameterService.API}/parameter-contexts/${configureParameterContext.id}/update-requests`,
            configureParameterContext.payload
        );
    }

    pollParameterContextUpdate(parameterContextId: string, requestId: string): Observable<any> {
        return this.httpClient.get(
            `${ParameterService.API}/parameter-contexts/${parameterContextId}/update-requests/${requestId}`
        );
    }

    deleteParameterContextUpdate(parameterContextId: string, requestId: string): Observable<any> {
        const params = new HttpParams({
            fromObject: {
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(
            `${ParameterService.API}/parameter-contexts/${parameterContextId}/update-requests/${requestId}`,
            { params }
        );
    }
}
