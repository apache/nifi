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

import { provideMockStore } from '@ngrx/store/testing';
import { RouterModule } from '@angular/router';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { Documentation } from './documentation.component';
import { extensionTypesFeatureKey } from '../../../state/extension-types';
import { initialState } from '../../../state/extension-types/extension-types.reducer';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

describe('Documentation', () => {
    let component: Documentation;
    let fixture: ComponentFixture<Documentation>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [Documentation],
            imports: [
                RouterModule,
                RouterTestingModule,
                MatAccordion,
                MatExpansionModule,
                FormsModule,
                MatFormFieldModule,
                MatInputModule,
                ReactiveFormsModule,
                NoopAnimationsModule,
                MockComponent(BannerText),
                MockComponent(Navigation)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [extensionTypesFeatureKey]: initialState
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(Documentation);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
