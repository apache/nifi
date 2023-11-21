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
import { BehaviorSubject, delay, Observable } from 'rxjs';

@Injectable({
    providedIn: 'root'
})
export class LoadingService {
    status$: Observable<boolean>;

    private loading: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(false);
    private requests: Map<string, boolean> = new Map<string, boolean>();

    constructor() {
        this.status$ = this.loading.asObservable().pipe(delay(0));
    }

    set(loading: boolean, url: string): void {
        if (loading) {
            this.requests.set(url, loading);
            this.loading.next(true);
            return;
        }

        if (!loading && this.requests.has(url)) {
            this.requests.delete(url);
        }

        if (this.requests.size === 0) {
            this.loading.next(false);
        }
    }
}
