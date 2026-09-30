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
import { flowFeatureKey, ImportFromRegistryDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState as initialFlowState } from '../../../../../state/flow/flow.reducer';
import { canvasFeatureKey } from '../../../../../state';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../../state/current-user';
import { flowConfigurationFeatureKey } from '../../../../../../../state/flow-configuration';
import { initialState as flowConfigurationInitialState } from '../../../../../../../state/flow-configuration/flow-configuration.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EMPTY, Observable, Subject } from 'rxjs';
import {
    BranchEntity,
    BucketEntity,
    RegistryClientEntity,
    VersionedFlowEntity,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../../../../state/shared';
import { HttpErrorResponse } from '@angular/common/http';
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
                        [flowConfigurationFeatureKey]: flowConfigurationInitialState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialFlowState
                        }
                    }
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: vi.fn()
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

    describe('with multiple registry clients', () => {
        const registryAId = 'registry-a';
        const registryBId = 'registry-b';
        const bucketAId = 'bucket-a';
        const bucketBId = 'bucket-b';

        let branchRequests: Map<string, Subject<BranchEntity[]>>;
        let bucketRequests: Map<string, Subject<BucketEntity[]>>;
        let flowRequests: Map<string, Subject<VersionedFlowEntity[]>>;
        let cancelledBucketRequests: string[];

        const createRegistryClient = (id: string, name: string, supportsBranching = false): RegistryClientEntity => {
            const template = data.registryClients[0];
            return {
                ...template,
                id,
                component: {
                    ...template.component,
                    id,
                    name,
                    supportsBranching
                }
            };
        };

        const createBucket = (id: string): BucketEntity => ({
            id,
            permissions: { canRead: true, canWrite: true },
            bucket: { id, created: 0, name: `bucket in ${id}`, description: '' }
        });

        const createBranch = (registryId: string): BranchEntity =>
            ({
                branch: { name: `branch in ${registryId}` }
            }) as BranchEntity;

        const createFlow = (bucketId: string): VersionedFlowEntity =>
            ({
                versionedFlow: {
                    flowId: `flow in ${bucketId}`,
                    flowName: `flow in ${bucketId}`,
                    description: ''
                }
            }) as VersionedFlowEntity;

        /**
         * Registers a request per key so that the test controls when each one responds, and records the
         * keys whose requests were torn down before completing.
         */
        const controllableRequest = <T>(requests: Map<string, Subject<T>>, cancelled?: string[]) => {
            return (key: string) =>
                new Observable<T>((subscriber) => {
                    const request = new Subject<T>();
                    requests.set(key, request);
                    const subscription = request.subscribe(subscriber);
                    return () => {
                        if (!request.closed) {
                            cancelled?.push(key);
                        }
                        subscription.unsubscribe();
                    };
                });
        };

        const createDialog = (supportsBranching: boolean) => {
            TestBed.resetTestingModule();
            TestBed.configureTestingModule({
                imports: [ImportFromRegistry, NoopAnimationsModule],
                providers: [
                    {
                        provide: MAT_DIALOG_DATA,
                        useValue: {
                            ...data,
                            registryClients: [
                                createRegistryClient(registryAId, 'A Registry', supportsBranching),
                                createRegistryClient(registryBId, 'B Registry', supportsBranching)
                            ]
                        }
                    },
                    provideMockStore({
                        initialState: {
                            [errorFeatureKey]: initialErrorState,
                            [currentUserFeatureKey]: initialCurrentUserState,
                            [flowConfigurationFeatureKey]: flowConfigurationInitialState,
                            [canvasFeatureKey]: {
                                [flowFeatureKey]: initialFlowState
                            }
                        }
                    }),
                    {
                        provide: ClusterConnectionService,
                        useValue: {
                            isDisconnectionAcknowledged: vi.fn()
                        }
                    },
                    { provide: MatDialogRef, useValue: null }
                ]
            });
            fixture = TestBed.createComponent(ImportFromRegistry);
            component = fixture.componentInstance;
            component.getBranches = controllableRequest(branchRequests);
            component.getBuckets = controllableRequest(bucketRequests, cancelledBucketRequests);
            component.getFlows = (registryId: string, bucketId: string) => controllableRequest(flowRequests)(bucketId);
            component.getFlowVersions = () => EMPTY;

            fixture.detectChanges();
        };

        beforeEach(() => {
            branchRequests = new Map<string, Subject<BranchEntity[]>>();
            bucketRequests = new Map<string, Subject<BucketEntity[]>>();
            flowRequests = new Map<string, Subject<VersionedFlowEntity[]>>();
            cancelledBucketRequests = [];
        });

        describe('that do not support branching', () => {
            beforeEach(() => {
                createDialog(false);
            });

            it('should ignore a bucket response for a registry that is no longer selected', () => {
                component.registryChanged(registryBId);

                bucketRequests.get(registryAId)?.next([createBucket(registryAId)]);

                expect(component.bucketOptions).toEqual([]);
                expect(component.importFromRegistryForm.get('bucket')?.value).toBeNull();

                bucketRequests.get(registryBId)?.next([createBucket(registryBId)]);

                expect(component.bucketOptions.map((option) => option.value)).toEqual([registryBId]);
                expect(component.importFromRegistryForm.get('bucket')?.value).toEqual(registryBId);
            });

            it('should cancel an outstanding bucket request when the selected registry changes', () => {
                expect(cancelledBucketRequests).toEqual([]);

                component.registryChanged(registryBId);

                expect(cancelledBucketRequests).toEqual([registryAId]);
            });

            it('should still load buckets for a later registry selection after a failed bucket request', () => {
                bucketRequests.get(registryAId)?.error(new Error('bucket request failed'));

                expect(component.loadingBuckets()).toBe(false);

                component.registryChanged(registryBId);
                bucketRequests.get(registryBId)?.next([createBucket(registryBId)]);

                expect(component.bucketOptions.map((option) => option.value)).toEqual([registryBId]);
                expect(component.importFromRegistryForm.get('bucket')?.value).toEqual(registryBId);
            });

            it('should still load flows for a later bucket selection after a failed flow request', () => {
                component.bucketChanged(bucketAId);
                flowRequests.get(bucketAId)?.error(new Error('flow request failed'));

                expect(component.loadingFlows()).toBe(false);

                component.bucketChanged(bucketBId);
                flowRequests.get(bucketBId)?.next([createFlow(bucketBId)]);

                expect(component.flowOptions.map((option) => option.value)).toEqual([`flow in ${bucketBId}`]);
                expect(component.importFromRegistryForm.get('flow')?.value).toEqual(`flow in ${bucketBId}`);
            });
        });

        describe('that support branching', () => {
            beforeEach(() => {
                createDialog(true);
            });

            it('should ignore a branch response for a registry that is no longer selected', () => {
                component.registryChanged(registryBId);

                branchRequests.get(registryAId)?.next([createBranch(registryAId)]);

                expect(component.branchOptions).toEqual([]);
                expect(component.importFromRegistryForm.get('branch')?.value).toBeNull();
            });

            it('should still load branches for a later registry selection after a failed branch request', () => {
                branchRequests.get(registryAId)?.error(new Error('branch request failed'));

                expect(component.loadingBranches()).toBe(false);

                component.registryChanged(registryBId);
                branchRequests.get(registryBId)?.next([createBranch(registryBId)]);

                expect(component.branchOptions.map((option) => option.value)).toEqual([`branch in ${registryBId}`]);
                expect(component.importFromRegistryForm.get('branch')?.value).toEqual(`branch in ${registryBId}`);
            });

            it('should re-enable the bucket control when its request is cancelled without a replacement', () => {
                // responding to the branch request auto-selects a branch, which issues the bucket request
                branchRequests.get(registryAId)?.next([createBranch(registryAId)]);

                expect(component.loadingBuckets()).toBe(true);
                expect(component.importFromRegistryForm.get('bucket')?.disabled).toBe(true);

                // changing registry re-issues only the branch level, so nothing replaces the cancelled
                // bucket request; the bucket control must not be left disabled
                component.registryChanged(registryBId);

                expect(component.loadingBuckets()).toBe(false);
                expect(component.importFromRegistryForm.get('bucket')?.disabled).toBe(false);
            });
        });
    });

    describe('when a versions request is superseded', () => {
        let versionRequests: Subject<VersionedFlowSnapshotMetadataEntity[]>;

        beforeEach(() => {
            versionRequests = new Subject<VersionedFlowSnapshotMetadataEntity[]>();
            component.getFlowVersions = () => versionRequests;
        });

        it('should clear the versions loading state when the request is cancelled without a replacement', () => {
            component.flowChanged('flow-1');
            expect(component.loadingVersions()).toBe(true);

            // selecting another bucket clears the flow selection, so no replacement versions request follows
            component.bucketChanged('bucket-1');

            expect(component.loadingVersions()).toBe(false);
        });

        it('should clear a previous versions error once the flow selection is cleared', () => {
            component.flowChanged('flow-1');
            versionRequests.error(new HttpErrorResponse({ status: 409, statusText: 'Conflict' }));

            expect(component.loadingVersions()).toBe(false);
            expect(component.loadingVersionsError()).not.toBeNull();

            component.bucketChanged('bucket-1');

            expect(component.loadingVersionsError()).toBeNull();
        });
    });
});
