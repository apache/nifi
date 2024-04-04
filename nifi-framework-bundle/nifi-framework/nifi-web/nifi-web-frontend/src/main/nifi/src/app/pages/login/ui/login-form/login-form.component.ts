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
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { AuthStorage } from '../../../../service/auth-storage.service';
import { Store } from '@ngrx/store';
import { LoginState } from '../../state';
import { login } from '../../state/access/access.actions';
import { AuthService } from '../../../../service/auth.service';

@Component({
    selector: 'login-form',
    templateUrl: './login-form.component.html',
    styleUrls: ['./login-form.component.scss']
})
export class LoginForm {
    loginForm: FormGroup;

    constructor(
        private formBuilder: FormBuilder,
        private store: Store<LoginState>,
        private authStorage: AuthStorage,
        private authService: AuthService
    ) {
        // build the form
        this.loginForm = this.formBuilder.group({
            username: new FormControl('', Validators.required),
            password: new FormControl('', Validators.required)
        });
    }

    hasToken(): boolean {
        return this.authStorage.hasToken();
    }

    logout(): void {
        this.authService.logout();
    }

    login() {
        const username: string = this.loginForm.get('username')?.value;
        const password: string = this.loginForm.get('password')?.value;

        this.store.dispatch(
            login({
                request: {
                    username,
                    password
                }
            })
        );
    }
}
