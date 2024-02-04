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

@Injectable({
    providedIn: 'root'
})
export class AuthStorage {
    private static readonly TOKEN_ITEM_KEY: string = 'Access-Token-Expiration';

    private static readonly REQUEST_TOKEN_PATTERN: RegExp = new RegExp('Request-Token=([^;]+)');

    /**
     * Get Request Token from document cookies
     *
     * @return Request Token string or null when not found
     */
    public getRequestToken(): string | null {
        const requestTokenMatcher = AuthStorage.REQUEST_TOKEN_PATTERN.exec(document.cookie);
        if (requestTokenMatcher) {
            return requestTokenMatcher[1];
        }
        return null;
    }

    /**
     * Get Token from Session Storage
     *
     * @return Bearer Token string
     */
    public getToken(): string | null {
        return sessionStorage.getItem(AuthStorage.TOKEN_ITEM_KEY);
    }

    /**
     * Has Token returns the status of whether Session Storage contains the Token
     *
     * @return Boolean status of whether Session Storage contains the Token
     */
    public hasToken(): boolean {
        return typeof this.getToken() === 'string';
    }

    /**
     * Remove Token from Session Storage
     *
     */
    public removeToken(): void {
        sessionStorage.removeItem(AuthStorage.TOKEN_ITEM_KEY);
    }

    /**
     * Set Token in Session Storage
     *
     * @param token Token String
     */
    public setToken(token: string): void {
        sessionStorage.setItem(AuthStorage.TOKEN_ITEM_KEY, token);
    }
}
