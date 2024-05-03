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

import { EditFlowAnalysisRule } from './edit-flow-analysis-rule.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EditFlowAnalysisRuleDialogRequest } from '../../../state/flow-analysis-rules';
import { Component } from '@angular/core';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/error/error.reducer';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

describe('EditFlowAnalysisRule', () => {
    let component: EditFlowAnalysisRule;
    let fixture: ComponentFixture<EditFlowAnalysisRule>;

    const data: EditFlowAnalysisRuleDialogRequest = {
        id: 'd5142be7-018c-1000-7105-2b1163fe0355',
        flowAnalysisRule: {
            revision: {
                clientId: '2be7f8d0-fad2-4909-918f-b9a4ef1675b2',
                version: 3
            },
            id: 'f08ddf27-018c-1000-4970-2fa78a6ee3ed',
            uri: 'https://localhost:8443/nifi-api/controller/flow-analysis-rules/f08ddf27-018c-1000-4970-2fa78a6ee3ed',
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            component: {
                id: 'f08ddf27-018c-1000-4970-2fa78a6ee3ed',
                name: 'DisallowComponentType',
                type: 'org.apache.nifi.flowanalysis.rules.DisallowComponentType',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-standard-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                state: 'DISABLED',
                comments: 'dfghsdgh',
                persistsState: false,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                enforcementPolicy: 'ENFORCE',
                properties: {
                    'component-type': null
                },
                descriptors: {
                    'component-type': {
                        name: 'component-type',
                        displayName: 'Component Type',
                        description:
                            "Components of the given type will produce a rule violation (i.e. they shouldn't exist). Either the simple or the fully qualified name of the type should be provided.",
                        required: true,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    }
                },
                validationErrors: ["'Component Type' is invalid because Component Type is required"],
                validationStatus: 'INVALID',
                extensionMissing: false
            },
            operatePermissions: {
                canRead: true,
                canWrite: true
            },
            status: {
                runStatus: 'DISABLED',
                validationStatus: 'INVALID'
            }
        }
    };

    @Component({
        selector: 'error-banner',
        standalone: true,
        template: ''
    })
    class MockErrorBanner {}

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditFlowAnalysisRule, MockErrorBanner, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({
                    initialState
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                }
            ]
        });
        fixture = TestBed.createComponent(EditFlowAnalysisRule);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
