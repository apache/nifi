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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { UserState } from './state/user';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectUser } from './state/user/user.selectors';
import { loadUser, startUserPolling, stopUserPolling } from './state/user/user.actions';

@Component({
    selector: 'nifi',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit, OnDestroy {
    title = 'nifi';

    constructor(private store: Store<UserState>) {
        this.store
            .select(selectUser)
            .pipe(takeUntilDestroyed())
            .subscribe((user) => {
                this.store.dispatch(startUserPolling());
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadUser());
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopUserPolling());
    }
}
