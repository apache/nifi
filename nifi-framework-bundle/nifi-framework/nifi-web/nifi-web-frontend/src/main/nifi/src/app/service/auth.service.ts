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
import { AuthStorage } from './auth-storage.service';

@Injectable({ providedIn: 'root' })
export class AuthService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private authStorage: AuthStorage
    ) {}

    public kerberos(): Observable<string> {
        return this.httpClient.post<string>(`${AuthService.API}/access/kerberos`, null);
    }

    public ticketExpiration(): Observable<any> {
        return this.httpClient.get(`${AuthService.API}/access/token/expiration`);
    }

    public accessConfig(): Observable<any> {
        return this.httpClient.get(`${AuthService.API}/access/config`);
    }

    public accessStatus(): Observable<any> {
        return this.httpClient.get(`${AuthService.API}/access`);
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
                this.authStorage.removeToken();
                window.location.href = './logout';
            });
    }

    /**
     * Extracts the subject from the specified jwt. If the jwt is not as expected
     * an empty string is returned.
     *
     * @param {string} jwt
     * @returns {string}
     */
    public getJwtPayload(jwt: string): any {
        if (jwt) {
            const segments: string[] = jwt.split(/\./);
            if (segments.length !== 3) {
                return null;
            }

            const rawPayload: string = atob(segments[1]);
            return JSON.parse(rawPayload);
        }

        return null;
    }

    /**
     * Get Session Expiration from JSON Web Token Payload exp claim
     *
     * @param {string} jwt
     * @return {string}
     */
    public getSessionExpiration(jwt: string): string | null {
        const jwtPayload = this.getJwtPayload(jwt);
        if (jwtPayload) {
            return jwtPayload['exp'];
        }

        return null;
    }

    /**
     * Get Default Session Expiration based on current time plus 12 hours as seconds
     *
     * @return {string}
     */
    public getDefaultExpiration(): string {
        const now: Date = new Date();
        const expiration: number = now.getTime() + 43200000;
        const expirationSeconds: number = Math.round(expiration / 1000);
        return expirationSeconds.toString();
    }
}
