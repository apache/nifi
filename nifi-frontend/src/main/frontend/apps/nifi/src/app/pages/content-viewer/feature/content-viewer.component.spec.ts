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

import { ContentViewerComponent } from './content-viewer.component';
import { provideMockStore } from '@ngrx/store/testing';
import { contentViewersFeatureKey } from '../state';
import { viewerOptionsFeatureKey } from '../state/viewer-options';
import { initialState } from '../state/viewer-options/viewer-options.reducer';
import { MatSelectModule } from '@angular/material/select';
import { ReactiveFormsModule } from '@angular/forms';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatIconModule } from '@angular/material/icon';
import { NifiTooltipDirective } from '@nifi/shared';

describe('ContentViewerComponent', () => {
    let component: ContentViewerComponent;
    let fixture: ComponentFixture<ContentViewerComponent>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [ContentViewerComponent],
            imports: [MatSelectModule, ReactiveFormsModule, NoopAnimationsModule, MatIconModule, NifiTooltipDirective],
            providers: [
                provideMockStore({
                    initialState: {
                        [contentViewersFeatureKey]: {
                            [viewerOptionsFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(ContentViewerComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
