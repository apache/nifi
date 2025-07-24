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
import { Client } from '../../../service/client.service';
import { Observable } from 'rxjs';
import { ResetCounterRequest } from '../state/counter-listing';

@Injectable({ providedIn: 'root' })
export class CountersService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client
    ) {}

    getCounters(): Observable<any> {
        return this.httpClient.get(`${CountersService.API}/counters`);
    }

    resetCounter(counterResetRequest: ResetCounterRequest): Observable<any> {
        return this.httpClient.put(`${CountersService.API}/counters/${counterResetRequest.counter.id}`, null);
    }

    resetAllCounters(): Observable<any> {
        return this.httpClient.put(`${CountersService.API}/counters`, null);
    }
}
