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

import { CreateParameterProvider } from './create-parameter-provider.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialParameterProvidersState } from '../../../state/parameter-providers/parameter-providers.reducer';
import { CreateParameterProviderDialogRequest } from '../../../state/parameter-providers';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('CreateParameterProvider', () => {
    let component: CreateParameterProvider;
    let fixture: ComponentFixture<CreateParameterProvider>;
    const data: CreateParameterProviderDialogRequest = {
        parameterProviderTypes: [
            {
                type: 'org.apache.nifi.parameter.aws.AwsSecretsManagerParameterProvider',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-aws-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                description:
                    'Fetches parameters from AWS SecretsManager.  Each secret becomes a Parameter group, which can map to a Parameter Context, with key/value pairs in the secret mapping to Parameters in the group.',
                restricted: false,
                tags: ['secretsmanager', 'manager', 'aws', 'secrets']
            },
            {
                type: 'org.apache.nifi.parameter.azure.AzureKeyVaultSecretsParameterProvider',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-azure-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                description:
                    "Fetches parameters from Azure Key Vault Secrets.  Each secret becomes a Parameter, which map to a Parameter Group byadding a secret tag named 'group-name'.",
                restricted: false,
                tags: ['keyvault', 'secrets', 'key', 'vault', 'azure']
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CreateParameterProvider, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                provideMockStore({ initialState: initialParameterProvidersState })
            ]
        });
        fixture = TestBed.createComponent(CreateParameterProvider);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
