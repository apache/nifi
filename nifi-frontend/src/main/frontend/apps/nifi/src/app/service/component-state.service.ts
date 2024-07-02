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
import { HttpClient } from '@angular/common/http';
import { ClearComponentStateRequest, LoadComponentStateRequest } from '../state/component-state';
import { Observable } from 'rxjs';
import { NiFiCommon } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class ComponentStateService {
    constructor(
        private httpClient: HttpClient,
        private nifiCommon: NiFiCommon
    ) {}

    getComponentState(request: LoadComponentStateRequest): Observable<any> {
        return this.httpClient.get(`${this.nifiCommon.stripProtocol(request.componentUri)}/state`);
    }

    clearComponentState(request: ClearComponentStateRequest): Observable<any> {
        return this.httpClient.post(`${this.nifiCommon.stripProtocol(request.componentUri)}/state/clear-requests`, {});
    }
}
