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

import { Component, inject } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { login, resetLoginFailure } from '../../state/access/access.actions';
import { selectLoginFailure, selectLoginPending } from '../../state/access/access.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { RouterLink } from '@angular/router';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { selectCurrentUser, selectLogoutSupported } from '../../../../state/current-user/current-user.selectors';
import { NiFiRegistryState } from '../../../../state';
import { logout } from '../../../../state/current-user/current-user.actions';

@Component({
    selector: 'nifi-registry-login-form',
    standalone: true,
    templateUrl: './login-form.component.html',
    styleUrls: ['./login-form.component.scss'],
    imports: [ReactiveFormsModule, RouterLink, MatFormFieldModule, MatInputModule, MatButtonModule]
})
export class LoginFormComponent {
    private store = inject<Store<NiFiRegistryState>>(Store);
    private formBuilder = inject(FormBuilder);

    loginFailure = this.store.selectSignal(selectLoginFailure);
    loginPending = this.store.selectSignal(selectLoginPending);
    currentUser = this.store.selectSignal(selectCurrentUser);
    logoutSupported = this.store.selectSignal(selectLogoutSupported);

    loginForm: FormGroup;

    constructor() {
        this.loginForm = this.formBuilder.group({
            username: new FormControl('', Validators.required),
            password: new FormControl('', Validators.required)
        });

        this.store
            .select(selectLoginFailure)
            .pipe(takeUntilDestroyed())
            .subscribe((failure) => {
                if (failure) {
                    this.loginForm.get('password')?.setValue('');
                    this.store.dispatch(resetLoginFailure());
                }
            });
    }

    login(): void {
        if (this.loginForm.invalid) {
            return;
        }

        const username = this.loginForm.get('username')?.value;
        const password = this.loginForm.get('password')?.value;

        this.store.dispatch(
            login({
                request: {
                    username,
                    password
                }
            })
        );
    }

    logout(): void {
        this.store.dispatch(logout());
    }
}
