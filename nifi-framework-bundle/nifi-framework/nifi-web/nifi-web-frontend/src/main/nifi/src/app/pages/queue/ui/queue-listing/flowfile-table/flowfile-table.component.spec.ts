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

import { FlowFileTable } from './flowfile-table.component';
import { MatTableModule } from '@angular/material/table';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Component } from '@angular/core';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/error/error.reducer';

describe('FlowFileTable', () => {
    let component: FlowFileTable;
    let fixture: ComponentFixture<FlowFileTable>;

    @Component({
        selector: 'error-banner',
        standalone: true,
        template: ''
    })
    class MockErrorBanner {}

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [FlowFileTable, MockErrorBanner, MatTableModule, NoopAnimationsModule],
            providers: [
                provideMockStore({
                    initialState
                })
            ]
        });
        fixture = TestBed.createComponent(FlowFileTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
