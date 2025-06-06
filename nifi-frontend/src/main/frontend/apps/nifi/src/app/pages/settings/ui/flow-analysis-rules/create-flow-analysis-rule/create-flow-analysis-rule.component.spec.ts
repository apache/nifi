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
import { MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { DocumentedType } from '../../../../../state/shared';
import { initialExtensionsTypesState } from '../../../../../state/extension-types/extension-types.reducer';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { MockComponent } from 'ng-mocks';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { of } from 'rxjs';

describe('CreateFlowAnalysisRule', () => {
    let component: CreateFlowAnalysisRule;
    let fixture: ComponentFixture<CreateFlowAnalysisRule>;

    const flowAnalysisRulesTaskTypes: DocumentedType[] = [
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
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                CreateFlowAnalysisRule,
                NoopAnimationsModule,
                MatIconTestingModule,
                MockComponent(ExtensionCreation)
            ],
            providers: [
                provideMockStore({ initialState: initialExtensionsTypesState }),
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateFlowAnalysisRule);
        component = fixture.componentInstance;
        component.flowAnalysisRulesTypes$ = of(flowAnalysisRulesTaskTypes);
        component.flowAnalysisRulesTypesLoadingStatus$ = of('success');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
