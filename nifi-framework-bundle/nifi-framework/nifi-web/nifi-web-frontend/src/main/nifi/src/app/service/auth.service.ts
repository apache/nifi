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
import { Observable, take } from 'rxjs';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Store } from '@ngrx/store';
import { NiFiState } from '../state';
import { navigateToLogOut } from '../state/current-user/current-user.actions';

@Injectable({ providedIn: 'root' })
export class AuthService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private store: Store<NiFiState>
    ) {}

    public getLoginConfiguration(): Observable<any> {
        return this.httpClient.get(`${AuthService.API}/authentication/configuration`);
    }

    public login(username: string, password: string): Observable<string> {
        const payload: HttpParams = new HttpParams().set('username', username).set('password', password);

        return this.httpClient.post(`${AuthService.API}/access/token`, payload, {
            responseType: 'text'
        });
    }

    public logout(): void {
        this.httpClient
            .delete(`${AuthService.API}/access/logout`)
            .pipe(take(1))
            .subscribe(() => {
                this.store.dispatch(navigateToLogOut());
            });
    }
}
