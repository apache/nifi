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

import { CreateProcessor } from './create-processor.component';
import { CreateProcessorDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialExtensionsTypesState } from '../../../../../../../state/extension-types/extension-types.reducer';
import { extensionTypesFeatureKey } from '../../../../../../../state/extension-types';
import { initialState as initialFlowState } from '../../../../../state/flow/flow.reducer';
import { flowFeatureKey } from '../../../../../state/flow';
import { ComponentType } from '@nifi/shared';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { DocumentedType } from '../../../../../../../state/shared';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { MockComponent } from 'ng-mocks';
import { ExtensionCreation } from '../../../../../../../ui/common/extension-creation/extension-creation.component';
import { of } from 'rxjs';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';

describe('CreateProcessor', () => {
    let component: CreateProcessor;
    let fixture: ComponentFixture<CreateProcessor>;

    const data: CreateProcessorDialogRequest = {
        request: {
            type: ComponentType.Processor,
            position: {
                x: 0,
                y: 0
            },
            revision: {
                version: 0,
                clientId: 'user'
            }
        }
    };

    const processorTypes: DocumentedType[] = [
        {
            type: 'org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-stateful-analysis-nar',
                version: '2.0.0-SNAPSHOT'
            },
            description:
                "Track a Rolling Window based on evaluating an Expression Language expression on each FlowFile and add that value to the processor's state. Each FlowFile will be emitted with the count of FlowFiles and total aggregate value of values processed in the current time window.",
            restricted: false,
            tags: ['rolling', 'data science', 'Attribute Expression Language', 'state', 'window']
        }
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateProcessor, NoopAnimationsModule, MatIconTestingModule, MockComponent(ExtensionCreation)],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [extensionTypesFeatureKey]: initialExtensionsTypesState,
                        canvas: {
                            [flowFeatureKey]: initialFlowState
                        }
                    }
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateProcessor);
        component = fixture.componentInstance;
        component.processorTypes$ = of(processorTypes);
        component.processorTypesLoadingStatus$ = of('success');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
