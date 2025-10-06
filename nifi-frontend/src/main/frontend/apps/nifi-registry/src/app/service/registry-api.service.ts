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
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { CurrentUser } from '../state/current-user';

@Injectable({ providedIn: 'root' })
export class RegistryApiService {
    private httpClient = inject(HttpClient);

    private static readonly API_BASE = '../nifi-registry-api';

    getCurrentUser(): Observable<CurrentUser> {
        return this.httpClient.get<CurrentUser>(`${RegistryApiService.API_BASE}/access`);
    }

    ticketExchangeKerberos(): Observable<string> {
        return this.httpClient.post(`${RegistryApiService.API_BASE}/access/token/kerberos`, null, {
            responseType: 'text'
        });
    }

    ticketExchangeOidc(): Observable<string> {
        return this.httpClient.post(`${RegistryApiService.API_BASE}/access/oidc/exchange`, null, {
            responseType: 'text',
            withCredentials: true
        });
    }

    login(username: string, password: string): Observable<string> {
        const headers = new HttpHeaders({
            Authorization: `Basic ${btoa(`${username}:${password}`)}`
        });

        return this.httpClient.post(`${RegistryApiService.API_BASE}/access/token/login`, null, {
            headers,
            withCredentials: true,
            responseType: 'text'
        });
    }

    logout(): Observable<any> {
        return this.httpClient.delete(`${RegistryApiService.API_BASE}/access/logout`, {
            withCredentials: true,
            responseType: 'text'
        });
    }
}
