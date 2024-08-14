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

import { StandardContentViewer } from './standard-content-viewer.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { DEFAULT_ROUTER_FEATURENAME } from '@ngrx/router-store';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { ReactiveFormsModule } from '@angular/forms';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';

describe('StandardContentViewer', () => {
    let component: StandardContentViewer;
    let fixture: ComponentFixture<StandardContentViewer>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [StandardContentViewer],
            imports: [HttpClientTestingModule, MatButtonToggleModule, ReactiveFormsModule, NgxSkeletonLoaderModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [DEFAULT_ROUTER_FEATURENAME]: {}
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(StandardContentViewer);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
