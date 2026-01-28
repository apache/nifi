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
import { SessionStorageService } from '@nifi/shared';

@Injectable({
    providedIn: 'root'
})
export class StorageService {
    private sessionStorageService = inject(SessionStorageService);

    private static readonly RETURN_URL = 'returnUrl';
    private static readonly CLIENT_ID = 'clientId';

    public setReturnUrl(url: string): void {
        this.sessionStorageService.setItem(StorageService.RETURN_URL, url);
    }

    public getReturnUrl(): string | null {
        return this.sessionStorageService.getItem<string>(StorageService.RETURN_URL);
    }

    public removeReturnUrl(): void {
        this.sessionStorageService.removeItem(StorageService.RETURN_URL);
    }

    public getClientId(): string | null {
        return this.sessionStorageService.getItem<string>(StorageService.CLIENT_ID);
    }

    public setClientId(clientId: string): void {
        this.sessionStorageService.setItem(StorageService.CLIENT_ID, clientId);
    }
}
