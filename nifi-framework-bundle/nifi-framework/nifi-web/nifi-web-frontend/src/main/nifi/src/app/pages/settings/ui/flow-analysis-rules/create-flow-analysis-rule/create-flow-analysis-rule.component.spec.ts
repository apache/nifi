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

import { CreateFlowAnalysisRule } from './create-flow-analysis-rule.component';
import { CreateFlowAnalysisRuleDialogRequest } from '../../../state/flow-analysis-rules';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { initialState } from '../../../state/flow-analysis-rules/flow-analysis-rules.reducer';

describe('CreateFlowAnalysisRule', () => {
    let component: CreateFlowAnalysisRule;
    let fixture: ComponentFixture<CreateFlowAnalysisRule>;

    const data: CreateFlowAnalysisRuleDialogRequest = {
        flowAnalysisRuleTypes: [
            {
                type: 'org.apache.nifi.flowanalysis.rules.DisallowComponentType',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-standard-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                description:
                    'Produces rule violations for each component (i.e. processors or controller services) of a given type.',
                restricted: false,
                tags: ['component', 'controller service', 'type', 'processor']
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateFlowAnalysisRule, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }, provideMockStore({ initialState })]
        });
        fixture = TestBed.createComponent(CreateFlowAnalysisRule);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
