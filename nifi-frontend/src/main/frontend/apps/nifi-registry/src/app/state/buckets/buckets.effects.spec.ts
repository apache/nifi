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

import { TestBed } from '@angular/core/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { Action } from '@ngrx/store';
import { Observable, of, throwError, Subject } from 'rxjs';
import { BucketsEffects } from './buckets.effects';
import { BucketsService } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { MatDialog } from '@angular/material/dialog';
import * as BucketsActions from './buckets.actions';
import * as PoliciesActions from '../policies/policies.actions';
import * as ErrorActions from '../error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorContextKey } from '../error';
import { CreateBucketDialogComponent } from '../../pages/buckets/feature/ui/create-bucket-dialog/create-bucket-dialog.component';
import { ManageBucketPoliciesDialogComponent } from '../../pages/buckets/feature/ui/manage-bucket-policies-dialog/manage-bucket-policies-dialog.component';
import { YesNoDialog } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { provideMockStore } from '@ngrx/store/testing';
import { Bucket } from './index';

const createBucket = (overrides = {}): Bucket => ({
    identifier: 'bucket-1',
    name: 'Test Bucket',
    description: 'Test Description',
    allowBundleRedeploy: false,
    allowPublicRead: false,
    createdTimestamp: 1632924000000,
    revision: {
        version: 1
    },
    permissions: {
        canRead: true,
        canWrite: true
    },
    link: {
        href: '/nifi-registry-api/buckets/bucket-1',
        params: {
            rel: 'self'
        }
    },
    ...overrides
});

describe('BucketsEffects', () => {
    let actions$: Observable<Action>;
    let effects: BucketsEffects;
    let bucketsService: jest.Mocked<BucketsService>;
    let errorHelper: jest.Mocked<ErrorHelper>;
    let dialog: jest.Mocked<MatDialog>;
    let store: Store;

    beforeEach(() => {
        const mockBucketsService = {
            getBuckets: jest.fn(),
            createBucket: jest.fn(),
            updateBucket: jest.fn(),
            deleteBucket: jest.fn()
        };

        const mockErrorHelper = {
            getErrorString: jest.fn()
        };

        const mockDialog = {
            open: jest.fn(),
            closeAll: jest.fn()
        };

        TestBed.configureTestingModule({
            providers: [
                BucketsEffects,
                provideMockActions(() => actions$),
                provideMockStore(),
                { provide: BucketsService, useValue: mockBucketsService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: MatDialog, useValue: mockDialog }
            ]
        });

        effects = TestBed.inject(BucketsEffects);
        bucketsService = TestBed.inject(BucketsService) as jest.Mocked<BucketsService>;
        errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
        dialog = TestBed.inject(MatDialog) as jest.Mocked<MatDialog>;
        store = TestBed.inject(Store);
        jest.spyOn(store, 'dispatch');
    });

    describe('loadBuckets$', () => {
        it('should return loadBucketsSuccess with buckets on success', (done) => {
            const buckets = [createBucket(), createBucket({ identifier: 'bucket-2' })];
            bucketsService.getBuckets.mockReturnValue(of(buckets));

            actions$ = of(BucketsActions.loadBuckets());

            effects.loadBuckets$.subscribe((action) => {
                expect(action).toEqual(
                    BucketsActions.loadBucketsSuccess({
                        response: { buckets }
                    })
                );
                done();
            });
        });

        it('should return error action on failure', (done) => {
            const error = new HttpErrorResponse({ status: 404, statusText: 'Not Found' });
            bucketsService.getBuckets.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error loading buckets');

            actions$ = of(BucketsActions.loadBuckets());

            effects.loadBuckets$.subscribe((action) => {
                expect(action).toEqual(
                    ErrorActions.addBannerError({
                        errorContext: {
                            errors: ['Error loading buckets'],
                            context: ErrorContextKey.GLOBAL
                        }
                    })
                );
                done();
            });
        });
    });

    describe('openCreateBucketDialog$', () => {
        it('should open create bucket dialog', (done) => {
            actions$ = of(BucketsActions.openCreateBucketDialog());

            effects.openCreateBucketDialog$.subscribe(() => {
                expect(dialog.open).toHaveBeenCalledWith(
                    CreateBucketDialogComponent,
                    expect.objectContaining({
                        autoFocus: false
                    })
                );
                done();
            });
        });
    });

    describe('createBucket$', () => {
        const bucket = createBucket();
        const request = {
            name: bucket.name,
            description: bucket.description,
            allowPublicRead: bucket.allowPublicRead
        };

        it('should return createBucketSuccess on success', (done) => {
            bucketsService.createBucket.mockReturnValue(of(bucket));

            actions$ = of(BucketsActions.createBucket({ request, keepDialogOpen: false }));

            effects.createBucket$.subscribe((action) => {
                expect(action).toEqual(
                    BucketsActions.createBucketSuccess({
                        response: bucket,
                        keepDialogOpen: false
                    })
                );
                done();
            });
        });

        it('should return error actions on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            bucketsService.createBucket.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error creating bucket');

            actions$ = of(BucketsActions.createBucket({ request, keepDialogOpen: false }));

            let actionCount = 0;
            effects.createBucket$.subscribe((action) => {
                if (actionCount === 0) {
                    expect(action).toEqual(BucketsActions.createBucketFailure());
                } else {
                    expect(action).toEqual(
                        ErrorActions.addBannerError({
                            errorContext: {
                                errors: ['Error creating bucket'],
                                context: ErrorContextKey.CREATE_BUCKET
                            }
                        })
                    );
                    done();
                }
                actionCount++;
            });
        });
    });

    describe('updateBucket$', () => {
        const bucket = createBucket();
        const request = { bucket };

        it('should return updateBucketSuccess on success', (done) => {
            bucketsService.updateBucket.mockReturnValue(of(bucket));

            actions$ = of(BucketsActions.updateBucket({ request }));

            effects.updateBucket$.subscribe((action) => {
                expect(action).toEqual(
                    BucketsActions.updateBucketSuccess({
                        response: bucket
                    })
                );
                done();
            });
        });

        it('should return error actions on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            bucketsService.updateBucket.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error updating bucket');

            actions$ = of(BucketsActions.updateBucket({ request }));

            let actionCount = 0;
            effects.updateBucket$.subscribe((action) => {
                if (actionCount === 0) {
                    expect(action).toEqual(BucketsActions.updateBucketFailure());
                } else {
                    expect(action).toEqual(
                        ErrorActions.addBannerError({
                            errorContext: {
                                errors: ['Error updating bucket'],
                                context: ErrorContextKey.UPDATE_BUCKET
                            }
                        })
                    );
                    done();
                }
                actionCount++;
            });
        });
    });

    describe('deleteBucket$', () => {
        const bucket = createBucket();
        const request = { bucket, version: bucket.revision.version };

        it('should return deleteBucketSuccess on success', (done) => {
            bucketsService.deleteBucket.mockReturnValue(of(bucket));

            actions$ = of(BucketsActions.deleteBucket({ request }));

            effects.deleteBucket$.subscribe((action) => {
                expect(action).toEqual(
                    BucketsActions.deleteBucketSuccess({
                        response: bucket
                    })
                );
                done();
            });
        });

        it('should return error actions on failure', (done) => {
            const error = new HttpErrorResponse({ status: 400, statusText: 'Bad Request' });
            bucketsService.deleteBucket.mockReturnValue(throwError(() => error));
            errorHelper.getErrorString.mockReturnValue('Error deleting bucket');

            actions$ = of(BucketsActions.deleteBucket({ request }));

            let actionCount = 0;
            effects.deleteBucket$.subscribe((action) => {
                if (actionCount === 0) {
                    expect(action).toEqual(BucketsActions.deleteBucketFailure());
                } else {
                    expect(action).toEqual(
                        ErrorActions.snackBarError({
                            error: 'Error deleting bucket'
                        })
                    );
                    done();
                }
                actionCount++;
            });
        });
    });

    describe('openDeleteBucketDialog$', () => {
        it('should open delete confirmation dialog', (done) => {
            const bucket = createBucket();
            const mockDialogRef = {
                componentInstance: {
                    yes: of(true)
                }
            };
            dialog.open.mockReturnValue(mockDialogRef as any);

            actions$ = of(BucketsActions.openDeleteBucketDialog({ request: { bucket } }));

            effects.openDeleteBucketDialog$.subscribe(() => {
                expect(dialog.open).toHaveBeenCalledWith(
                    YesNoDialog,
                    expect.objectContaining({
                        data: {
                            title: 'Delete Bucket',
                            message: 'All items stored in this bucket will be deleted as well.'
                        }
                    })
                );
                expect(store.dispatch).toHaveBeenCalledWith(
                    BucketsActions.deleteBucket({
                        request: {
                            bucket,
                            version: bucket.revision.version
                        }
                    })
                );
                done();
            });
        });
    });

    describe('openManageBucketPoliciesDialog$', () => {
        it('should dispatch load actions and open dialog when both succeed', (done) => {
            const bucket = createBucket();
            const mockDialogRef = {
                componentInstance: {
                    savePolicies: {
                        pipe: jest.fn().mockReturnValue({ subscribe: jest.fn() })
                    }
                },
                afterClosed: jest.fn().mockReturnValue(of(undefined))
            };

            dialog.open.mockReturnValue(mockDialogRef as any);

            // Use a Subject to control when actions are emitted
            const actionsSubject = new Subject<Action>();
            actions$ = actionsSubject.asObservable();

            // Subscribe to the effect
            effects.openManageBucketPoliciesDialog$.subscribe(() => {
                expect(dialog.open).toHaveBeenCalledWith(
                    ManageBucketPoliciesDialogComponent,
                    expect.objectContaining({
                        autoFocus: false,
                        data: expect.objectContaining({
                            bucket
                        })
                    })
                );
                expect(store.dispatch).toHaveBeenCalledTimes(2); // loadPolicyTenants + loadPolicies
                done();
            });

            // Emit the initial action to trigger the effect
            actionsSubject.next(BucketsActions.openManageBucketPoliciesDialog({ request: { bucket } }));

            // Allow the effect to set up listeners, then emit success actions
            setTimeout(() => {
                actionsSubject.next(
                    PoliciesActions.loadPolicyTenantsSuccess({ response: { users: [], userGroups: [] } })
                );
                actionsSubject.next(
                    PoliciesActions.loadPoliciesSuccess({
                        response: { bucketId: bucket.identifier, policies: [] }
                    })
                );
            }, 10);
        });
    });

    describe('dialog closing effects', () => {
        it('should close dialogs on createBucketSuccess when keepDialogOpen is false', (done) => {
            actions$ = of(BucketsActions.createBucketSuccess({ response: createBucket(), keepDialogOpen: false }));

            effects.createBucketSuccess$.subscribe(() => {
                expect(dialog.closeAll).toHaveBeenCalled();
                done();
            });
        });

        it('should not close dialogs on createBucketSuccess when keepDialogOpen is true', (done) => {
            actions$ = of(BucketsActions.createBucketSuccess({ response: createBucket(), keepDialogOpen: true }));

            effects.createBucketSuccess$.subscribe(() => {
                expect(dialog.closeAll).not.toHaveBeenCalled();
                done();
            });
        });

        it('should close dialogs on updateBucketSuccess', (done) => {
            actions$ = of(BucketsActions.updateBucketSuccess({ response: createBucket() }));

            effects.updateBucketSuccess$.subscribe(() => {
                expect(dialog.closeAll).toHaveBeenCalled();
                done();
            });
        });

        it('should close dialogs on deleteBucketSuccess', (done) => {
            actions$ = of(BucketsActions.deleteBucketSuccess({ response: createBucket() }));

            effects.deleteBucketSuccess$.subscribe(() => {
                expect(dialog.closeAll).toHaveBeenCalled();
                done();
            });
        });
    });
});
