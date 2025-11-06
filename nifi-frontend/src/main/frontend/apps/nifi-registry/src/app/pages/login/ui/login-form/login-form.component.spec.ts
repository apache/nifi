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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { LoginFormComponent } from './login-form.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { NiFiRegistryState } from '../../../../state';
import { login, resetLoginFailure } from '../../state/access/access.actions';
import { logout } from '../../../../state/current-user/current-user.actions';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { RouterTestingModule } from '@angular/router/testing';
import { selectLoginFailure } from '../../state/access/access.selectors';
import { initialState as initialCurrentUserState } from '../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../state/current-user';

describe('LoginFormComponent', () => {
    let component: LoginFormComponent;
    let fixture: ComponentFixture<LoginFormComponent>;
    let store: MockStore<NiFiRegistryState>;
    let dispatchSpy: jest.SpyInstance;
    let loginFailureSelector: any;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                LoginFormComponent,
                ReactiveFormsModule,
                MatFormFieldModule,
                MatInputModule,
                MatButtonModule,
                RouterTestingModule
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [currentUserFeatureKey]: initialCurrentUserState
                    }
                }),
                provideHttpClientTesting()
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(LoginFormComponent);
        component = fixture.componentInstance;
        store = TestBed.inject(MockStore);
        loginFailureSelector = store.overrideSelector(selectLoginFailure, null);
        dispatchSpy = jest.spyOn(store, 'dispatch');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should dispatch login action when form is valid and submitted', () => {
        dispatchSpy.mockClear();
        component.loginForm.setValue({ username: 'user', password: 'pass' });
        component.login();
        expect(store.dispatch).toHaveBeenCalledWith(
            login({
                request: {
                    username: 'user',
                    password: 'pass'
                }
            })
        );
    });

    it('should not dispatch login when form is invalid', () => {
        dispatchSpy.mockClear();
        component.loginForm.setValue({ username: '', password: '' });
        component.login();
        expect(store.dispatch).not.toHaveBeenCalled();
    });

    it('should dispatch logout when logout is called', () => {
        dispatchSpy.mockClear();
        component.logout();
        expect(store.dispatch).toHaveBeenCalledWith(logout());
    });

    it('should dispatch resetLoginFailure when login failure occurs', () => {
        dispatchSpy.mockClear();
        loginFailureSelector.setResult('Invalid credentials');
        store.refreshState();
        expect(store.dispatch).toHaveBeenCalledWith(resetLoginFailure());
    });
});
