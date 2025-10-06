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
import { FlowAnalysisDrawerComponent } from './flow-analysis-drawer.component';
import { provideMockStore } from '@ngrx/store/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { initialState as initialErrorState } from '../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../state/error';
import { initialState as initialFlowAnalysisState } from '../../../../state/flow-analysis/flow-analysis.reducer';
import { flowAnalysisFeatureKey } from '../../../../state/flow-analysis';
import { initialState as initialFlowState } from '../../../../state/flow/flow.reducer';
import { flowFeatureKey } from '../../../../state/flow';

describe('FlowAnalysisDrawerComponent', () => {
    let component: FlowAnalysisDrawerComponent;
    let fixture: ComponentFixture<FlowAnalysisDrawerComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [FlowAnalysisDrawerComponent, NoopAnimationsModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        canvas: {
                            [flowAnalysisFeatureKey]: initialFlowAnalysisState,
                            [flowFeatureKey]: initialFlowState
                        }
                    }
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(FlowAnalysisDrawerComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
