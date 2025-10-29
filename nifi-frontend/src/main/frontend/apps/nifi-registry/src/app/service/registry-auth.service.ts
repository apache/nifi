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
import { Storage } from '@nifi/shared';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { RegistryApiService } from './registry-api.service';

interface JwtPayload {
    exp?: number;
}

@Injectable({ providedIn: 'root' })
export class RegistryAuthService {
    private storage = inject(Storage);
    private registryApi = inject(RegistryApiService);

    private static readonly API_BASE = '../nifi-registry-api';
    private static readonly JWT_STORAGE_KEY = 'jwt';
    private static readonly REDIRECT_STORAGE_KEY = 'nifi-registry-redirect-url';
    private static readonly REQUEST_TOKEN_PATTERN = /Request-Token=([^;]+)/;

    ticketExchange(): Observable<string> {
        const storedToken = this.getStoredToken();
        if (storedToken) {
            return of(storedToken);
        }

        return this.registryApi.ticketExchangeKerberos().pipe(
            map((jwt) => this.handleJwt(jwt)),
            catchError(() =>
                this.registryApi.ticketExchangeOidc().pipe(
                    map((jwt) => this.handleJwt(jwt)),
                    catchError(() => of(''))
                )
            )
        );
    }

    login(username: string, password: string): Observable<string> {
        return this.registryApi.login(username, password).pipe(map((jwt) => this.handleJwt(jwt)));
    }

    getStoredToken(): string | null {
        return this.storage.getItem<string>(RegistryAuthService.JWT_STORAGE_KEY);
    }

    clearToken(): void {
        this.storage.removeItem(RegistryAuthService.JWT_STORAGE_KEY);
    }

    setRedirectUrl(url: string): void {
        try {
            window.sessionStorage.setItem(RegistryAuthService.REDIRECT_STORAGE_KEY, url);
        } catch (error) {
            /* empty */
        }
    }

    consumeRedirectUrl(): string | null {
        try {
            const url = window.sessionStorage.getItem(RegistryAuthService.REDIRECT_STORAGE_KEY);
            window.sessionStorage.removeItem(RegistryAuthService.REDIRECT_STORAGE_KEY);
            return url;
        } catch (error) {
            return null;
        }
    }

    storeToken(jwt: string): void {
        this.handleJwt(jwt);
    }

    logout(): Observable<any> {
        return this.registryApi.logout();
    }

    getRequestTokenFromCookies(): string | null {
        try {
            const matcher = RegistryAuthService.REQUEST_TOKEN_PATTERN.exec(document.cookie);
            if (matcher && matcher[1]) {
                return matcher[1];
            }
        } catch (error) {
            /* empty */
        }
        return null;
    }

    private handleJwt(jwt: string): string {
        if (!jwt) {
            return jwt;
        }

        const payload = this.parseJwt(jwt);
        const expiration = payload?.exp ? payload.exp * 1000 : undefined;
        this.storage.setItem(RegistryAuthService.JWT_STORAGE_KEY, jwt, expiration);
        return jwt;
    }

    private parseJwt(token: string): JwtPayload | null {
        try {
            const parts = token.split('.');
            if (parts.length !== 3) {
                return null;
            }
            const normalized = parts[1].replace(/-/g, '+').replace(/_/g, '/');
            const payload = window.atob(normalized);
            return JSON.parse(payload);
        } catch (error) {
            return null;
        }
    }
}
