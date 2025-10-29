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
import { MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialExtensionsTypesState } from '../../../../state/extension-types/extension-types.reducer';
import { extensionTypesFeatureKey } from '../../../../state/extension-types';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { MockComponent } from 'ng-mocks';
import { ExtensionCreation } from '../../extension-creation/extension-creation.component';
import { DocumentedType } from '../../../../state/shared';
import { of } from 'rxjs';
import { initialState as initialErrorState } from '../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../state/error';

describe('CreateControllerService', () => {
    let component: CreateControllerService;
    let fixture: ComponentFixture<CreateControllerService>;

    const controllerServiceTypes: DocumentedType[] = [
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
    ];

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                CreateControllerService,
                NoopAnimationsModule,
                MatIconTestingModule,
                MockComponent(ExtensionCreation)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [extensionTypesFeatureKey]: initialExtensionsTypesState
                    }
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(CreateControllerService);
        component = fixture.componentInstance;
        component.controllerServiceTypes$ = of(controllerServiceTypes);
        component.controllerServiceTypesLoadingStatus$ = of('success');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
