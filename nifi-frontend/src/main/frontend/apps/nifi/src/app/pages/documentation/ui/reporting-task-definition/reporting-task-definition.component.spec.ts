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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ReportingTaskDefinition } from './reporting-task-definition.component';
import { provideMockStore } from '@ngrx/store/testing';
import { documentationFeatureKey } from '../../state';
import { reportingTaskDefinitionFeatureKey } from '../../state/reporting-task-definition';
import { initialState } from '../../state/reporting-task-definition/reporting-task-definition.reducer';

describe('ReportingTaskDefinition', () => {
    let component: ReportingTaskDefinition;
    let fixture: ComponentFixture<ReportingTaskDefinition>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ReportingTaskDefinition],
            providers: [
                provideMockStore({
                    initialState: {
                        [documentationFeatureKey]: {
                            [reportingTaskDefinitionFeatureKey]: initialState
                        }
                    }
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ReportingTaskDefinition);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
