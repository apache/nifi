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

import { ImportFromRegistry } from './import-from-registry.component';
import { ImportFromRegistryDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState as initialFlowState } from '../../../../../state/flow/flow.reducer';
import { flowFeatureKey } from '../../../../../state/flow';
import { canvasFeatureKey } from '../../../../../state';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../../state/current-user';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EMPTY } from 'rxjs';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { By } from '@angular/platform-browser';

describe('ImportFromRegistry', () => {
    let component: ImportFromRegistry;
    let fixture: ComponentFixture<ImportFromRegistry>;

    const data: ImportFromRegistryDialogRequest = {
        request: {
            revision: {
                clientId: '88cd6620-bd6d-41fa-aa5a-be2b33501e31',
                version: 0
            },
            type: ComponentType.Flow,
            position: {
                x: 461,
                y: 58
            }
        },
        registryClients: [
            {
                revision: {
                    version: 0
                },
                id: '6a088515-018d-1000-ce79-5ae44266bc20',
                uri: 'https://localhost:4200/nifi-api/controller/registry-clients/6a088515-018d-1000-ce79-5ae44266bc20',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '6a088515-018d-1000-ce79-5ae44266bc20',
                    name: 'My Registry',
                    description: '',
                    type: 'org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient',
                    bundle: {
                        group: 'org.apache.nifi',
                        artifact: 'nifi-flow-registry-client-nar',
                        version: '2.0.0-SNAPSHOT'
                    },
                    properties: {
                        url: 'http://localhost:18080/nifi-registry',
                        'ssl-context-service': null
                    },
                    descriptors: {
                        url: {
                            name: 'url',
                            displayName: 'URL',
                            description: 'URL of the NiFi Registry',
                            required: true,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            dependencies: []
                        },
                        'ssl-context-service': {
                            name: 'ssl-context-service',
                            displayName: 'SSL Context Service',
                            description: 'Specifies the SSL Context Service to use for communicating with NiFiRegistry',
                            allowableValues: [],
                            required: false,
                            sensitive: false,
                            dynamic: false,
                            supportsEl: false,
                            expressionLanguageScope: 'Not Supported',
                            identifiesControllerService: 'org.apache.nifi.ssl.SSLContextService',
                            identifiesControllerServiceBundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-standard-services-api-nar',
                                version: '2.0.0-SNAPSHOT'
                            },
                            dependencies: []
                        }
                    },
                    supportsSensitiveDynamicProperties: false,
                    restricted: false,
                    deprecated: false,
                    validationStatus: 'VALID',
                    multipleVersionsAvailable: false,
                    extensionMissing: false
                }
            }
        ]
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ImportFromRegistry, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialFlowState
                        }
                    }
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: jest.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(ImportFromRegistry);
        component = fixture.componentInstance;

        component.getBuckets = () => {
            return EMPTY;
        };
        component.getFlows = () => {
            return EMPTY;
        };
        component.getFlowVersions = () => {
            return EMPTY;
        };

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should show the skeleton loader for versions', () => {
        component.loadingVersions.set(true);
        fixture.detectChanges();
        const skeleton = fixture.debugElement.query(By.css('div[data-qa="skeleton-loader-versions"]'));
        const error = fixture.debugElement.query(By.css('div[data-qa="loading-versions-error"]'));
        expect(skeleton).toBeTruthy();
        expect(error).toBeFalsy();
    });

    it('should show the loading error panel if there is an error', () => {
        component.loadingVersions.set(false);
        component.loadingVersionsError.set('some error happened');
        fixture.detectChanges();
        const skeleton = fixture.debugElement.query(By.css('div[data-qa="skeleton-loader-versions"]'));
        const error = fixture.debugElement.query(By.css('div[data-qa="loading-versions-error"]'));
        const versions = fixture.debugElement.query(By.css('div[data-qa="versions-listing-table"]'));
        expect(skeleton).toBeFalsy();
        expect(error).toBeTruthy();
        expect(versions).toBeFalsy();
    });

    it('should show the versions', () => {
        component.loadingVersions.set(false);
        fixture.detectChanges();
        const skeleton = fixture.debugElement.query(By.css('div[data-qa="skeleton-loader-versions"]'));
        const error = fixture.debugElement.query(By.css('div[data-qa="loading-versions-error"]'));
        const versions = fixture.debugElement.query(By.css('div[data-qa="versions-listing-table"]'));
        expect(skeleton).toBeFalsy();
        expect(error).toBeFalsy();
        expect(versions).toBeTruthy();
    });
});
