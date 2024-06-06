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

import { GeneralForm } from './general-form.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/general/general.reducer';
import { MatFormFieldModule } from '@angular/material/form-field';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

describe('GeneralForm', () => {
    let component: GeneralForm;
    let fixture: ComponentFixture<GeneralForm>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [GeneralForm],
            imports: [NoopAnimationsModule, MatFormFieldModule, MatInputModule, FormsModule, ReactiveFormsModule],
            providers: [
                provideMockStore({
                    initialState
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                }
            ]
        });
        fixture = TestBed.createComponent(GeneralForm);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
