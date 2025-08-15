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

import { JoltTransformJsonUi } from './jolt-transform-json-ui.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../state/jolt-transform-json-processor-details/jolt-transform-json-processor-details.reducer';
import { joltTransformJsonProcessorDetailsFeatureKey, joltTransformJsonUiFeatureKey } from '../state';

describe('jolt-transform-json-ui', () => {
    let component: JoltTransformJsonUi;
    let fixture: ComponentFixture<JoltTransformJsonUi>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [JoltTransformJsonUi],
            providers: [
                provideMockStore({
                    initialState: {
                        [joltTransformJsonUiFeatureKey]: {
                            [joltTransformJsonProcessorDetailsFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(JoltTransformJsonUi);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
