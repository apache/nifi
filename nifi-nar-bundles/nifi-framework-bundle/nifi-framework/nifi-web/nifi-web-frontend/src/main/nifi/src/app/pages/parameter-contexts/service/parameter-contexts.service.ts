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
import { Observable, throwError } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';
import {
    CreateParameterContextRequest,
    DeleteParameterContextRequest,
    ParameterContextEntity
} from '../state/parameter-context-listing';
import { ParameterContextUpdateRequest, SubmitParameterContextUpdate } from '../../../state/shared';

@Injectable({ providedIn: 'root' })
export class ParameterContextService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getParameterContexts(): Observable<any> {
        return this.httpClient.get(`${ParameterContextService.API}/flow/parameter-contexts`);
    }

    createParameterContext(createParameterContext: CreateParameterContextRequest): Observable<any> {
        return this.httpClient.post(
            `${ParameterContextService.API}/parameter-contexts`,
            createParameterContext.payload
        );
    }

    getParameterContext(id: string, includeInheritedParameters: boolean): Observable<any> {
        return this.httpClient.get(`${ParameterContextService.API}/parameter-contexts/${id}`, {
            params: {
                includeInheritedParameters
            }
        });
    }

    submitParameterContextUpdate(configureParameterContext: SubmitParameterContextUpdate): Observable<any> {
        return this.httpClient.post(
            `${ParameterContextService.API}/parameter-contexts/${configureParameterContext.id}/update-requests`,
            configureParameterContext.payload
        );
    }

    pollParameterContextUpdate(updateRequest: ParameterContextUpdateRequest): Observable<any> {
        return this.httpClient.get(this.nifiCommon.stripProtocol(updateRequest.uri));
    }

    deleteParameterContextUpdate(updateRequest: ParameterContextUpdateRequest): Observable<any> {
        return this.httpClient.delete(this.nifiCommon.stripProtocol(updateRequest.uri));
    }

    deleteParameterContext(deleteParameterContext: DeleteParameterContextRequest): Observable<any> {
        const entity: ParameterContextEntity = deleteParameterContext.parameterContext;
        const revision: any = this.client.getRevision(entity);
        return this.httpClient.delete(`${ParameterContextService.API}/parameter-contexts/${entity.id}`, {
            params: revision
        });
    }
}
