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

import { CreateControllerService } from './create-controller-service.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../state/extension-types/extension-types.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { CreateControllerServiceDialogRequest } from '../../../../state/shared';

describe('CreateControllerService', () => {
    let component: CreateControllerService;
    let fixture: ComponentFixture<CreateControllerService>;

    const data: CreateControllerServiceDialogRequest = {
        controllerServiceTypes: [
            {
                type: 'org.apache.nifi.services.azure.storage.ADLSCredentialsControllerService',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-azure-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                controllerServiceApis: [
                    {
                        type: 'org.apache.nifi.services.azure.storage.ADLSCredentialsService',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-azure-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        }
                    }
                ],
                description: 'Defines credentials for ADLS processors.',
                restricted: false,
                tags: ['cloud', 'credentials', 'adls', 'storage', 'microsoft', 'azure']
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateControllerService, NoopAnimationsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }, provideMockStore({ initialState })]
        });
        fixture = TestBed.createComponent(CreateControllerService);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
