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

import { FlowStatus } from './flow-status.component';
import { Search } from '../search/search.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

describe('FlowStatus', () => {
    let component: FlowStatus;
    let fixture: ComponentFixture<FlowStatus>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                FlowStatus,
                Search,
                HttpClientTestingModule,
                CdkOverlayOrigin,
                CdkConnectedOverlay,
                MatAutocompleteModule,
                FormsModule,
                ReactiveFormsModule
            ]
        });
        fixture = TestBed.createComponent(FlowStatus);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
