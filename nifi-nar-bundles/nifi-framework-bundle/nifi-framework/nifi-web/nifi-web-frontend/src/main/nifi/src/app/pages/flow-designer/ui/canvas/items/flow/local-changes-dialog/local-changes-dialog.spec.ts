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

import { LocalChangesDialog } from './local-changes-dialog';
import { LocalChangesDialogRequest } from '../../../../../state/flow';
import { ComponentType } from '../../../../../../../state/shared';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('LocalChangesDialog', () => {
    let component: LocalChangesDialog;
    let fixture: ComponentFixture<LocalChangesDialog>;
    const request: LocalChangesDialogRequest = {
        localModifications: {
            componentDifferences: [
                {
                    componentType: ComponentType.Processor,
                    componentId: '575f027f-018d-1000-b5e3-9cf78bff50ce',
                    componentName: 'LogAttribute',
                    processGroupId: '575e8ad2-018d-1000-fb2c-ea23f9efb273',
                    differences: [
                        {
                            differenceType: 'Property Value Changed',
                            difference: "From 'info' to 'warn'"
                        },
                        {
                            differenceType: 'Position Changed',
                            difference: 'Position was changed'
                        }
                    ]
                },
                {
                    componentType: ComponentType.Funnel,
                    componentId: '57607557-018d-1000-e45a-8a132d9965d2',
                    processGroupId: '575e8ad2-018d-1000-fb2c-ea23f9efb273',
                    differences: [
                        {
                            differenceType: 'Position Changed',
                            difference: 'Position was changed'
                        }
                    ]
                }
            ]
        },
        versionControlInformation: {
            processGroupRevision: {
                clientId: 'a54dacf3-018e-1000-ac75-2eeef71a49cc',
                version: 9
            },
            versionControlInformation: {
                groupId: '575e8ad2-018d-1000-fb2c-ea23f9efb273',
                registryId: '80441509-018e-1000-12b2-d70361a7f661',
                registryName: 'Local Registry',
                bucketId: 'bd6fa6cc-da95-4a12-92cc-9b38b3d48266',
                bucketName: 'RAG',
                flowId: '28dc4617-541c-4912-87c8-aad0ae882d33',
                flowName: 'asdfasdfa',
                flowDescription: 'asdf',
                version: 2,
                state: 'LOCALLY_MODIFIED_AND_STALE',
                stateExplanation: 'Local changes have been made and a newer version of this flow is available'
            }
        },
        mode: 'SHOW'
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [LocalChangesDialog, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: request
                }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(LocalChangesDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
