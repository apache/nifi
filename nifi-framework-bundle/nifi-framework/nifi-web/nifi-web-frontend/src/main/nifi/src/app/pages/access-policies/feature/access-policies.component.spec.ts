/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { AccessPolicies } from './access-policies.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../state/access-policy/access-policy.reducer';
import { Component } from '@angular/core';

describe('AccessPolicies', () => {
    let component: AccessPolicies;
    let fixture: ComponentFixture<AccessPolicies>;

    @Component({
        selector: 'navigation',
        standalone: true,
        template: ''
    })
    class MockNavigation {}

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [AccessPolicies],
            imports: [RouterModule, RouterTestingModule, MockNavigation],
            providers: [
                provideMockStore({
                    initialState
                })
            ]
        });
        fixture = TestBed.createComponent(AccessPolicies);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
