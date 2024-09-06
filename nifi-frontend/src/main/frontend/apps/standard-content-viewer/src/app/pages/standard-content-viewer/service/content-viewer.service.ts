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

@Injectable({ providedIn: 'root' })
export class ContentViewerService {
    private static readonly API: string = 'api';

    constructor(private httpClient: HttpClient) {}

    getContent(
        ref: string,
        mimeTypeDisplayName: string,
        formatted: string,
        clientId: string | undefined
    ): Observable<any> {
        let params = new HttpParams()
            .set('ref', ref)
            .set('mimeTypeDisplayName', mimeTypeDisplayName)
            .set('formatted', formatted);

        if (clientId) {
            params = params.set('clientId', clientId);
        }

        return this.httpClient.get(`${ContentViewerService.API}/content`, {
            params,
            responseType: 'text'
        });
    }
}
