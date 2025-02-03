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

import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import { LoginState } from '../state';
import { selectCurrentUserState } from '../../../state/current-user/current-user.selectors';
import { take } from 'rxjs';
import { selectLoginConfiguration } from '../../../state/login-configuration/login-configuration.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { isDefinedAndNotNull } from '@nifi/shared';

@Component({
    selector: 'login',
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.scss'],
    standalone: false
})
export class Login {
    currentUserState$ = this.store.select(selectCurrentUserState).pipe(take(1));
    loginConfiguration = this.store.selectSignal(selectLoginConfiguration);

    loading: boolean = true;

    constructor(private store: Store<LoginState>) {
        this.store
            .select(selectLoginConfiguration)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((loginConfiguration) => {
                if (loginConfiguration.externalLoginRequired) {
                    window.location.href = loginConfiguration.loginUri;
                } else {
                    this.loading = false;
                }
            });
    }
}
