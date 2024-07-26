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
import { Observable } from 'rxjs';
import { SavePropertiesRequest } from '../state/jolt-transform-json-property';
import { ValidateJoltSpecRequest } from '../state/jolt-transform-json-validate';

@Injectable({ providedIn: 'root' })
export class JoltTransformJsonUiService {
    private static readonly API: string = 'api';

    constructor(private httpClient: HttpClient) {}

    getProcessorDetails(id: string): Observable<any> {
        return this.httpClient.get(`${JoltTransformJsonUiService.API}/standard/processor/details?processorId=${id}`);
    }

    saveProperties(request: SavePropertiesRequest): Observable<any> {
        const params = new HttpParams()
            .set('processorId', request.processorId)
            .set('revisionId', request.revision)
            .set('clientId', request.clientId)
            .set('disconnectedNodeAcknowledged', request.disconnectedNodeAcknowledged);

        return this.httpClient.put(
            `${JoltTransformJsonUiService.API}/standard/processor/properties`,
            {
                'Jolt Specification': request['Jolt Specification'],
                'Jolt Transform': request['Jolt Transform']
            },
            { params }
        );
    }

    validateJoltSpec(payload: ValidateJoltSpecRequest): Observable<any> {
        return this.httpClient.post(`${JoltTransformJsonUiService.API}/standard/transformjson/validate`, payload);
    }

    transformJoltSpec(payload: ValidateJoltSpecRequest): Observable<any> {
        return this.httpClient.post(`${JoltTransformJsonUiService.API}/standard/transformjson/execute`, payload);
    }
}
