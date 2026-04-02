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
import { Observable, of, throwError } from 'rxjs';
import { PoliciesEffects } from './policies.effects';
import { BucketsService, Policy, PolicySubject } from '../../service/buckets.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { MatSnackBar } from '@angular/material/snack-bar';
import * as PoliciesActions from './policies.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorContextKey } from '../error';

const createPolicy = (overrides = {}): Policy => ({
    identifier: 'policy-1',
    action: 'read',
    resource: '/buckets/bucket-1',
    users: [],
    userGroups: [],
    revision: { version: 0 },
    configurable: true,
    ...overrides
});

const createPolicySubject = (overrides = {}): PolicySubject => ({
    identifier: 'user-1',
    identity: 'test-user',
    type: 'user',
    configurable: false,
    ...overrides
});

describe('PoliciesEffects', () => {
    let actions$: Observable<Action>;
    let effects: PoliciesEffects;
    let bucketsService: vi.Mocked<BucketsService>;
    let errorHelper: vi.Mocked<ErrorHelper>;
    let snackBar: vi.Mocked<MatSnackBar>;

    beforeEach(() => {
        const mockBucketsService = {
            getPolicies: vi.fn(),
            getBucketPolicyTenants: vi.fn(),
            saveBucketPolicy: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn()
        };

        const mockSnackBar = {
            open: vi.fn()
        };

        TestBed.configureTestingModule({
            providers: [
                PoliciesEffects,
                provideMockActions(() => actions$),
                { provide: BucketsService, useValue: mockBucketsService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: MatSnackBar, useValue: mockSnackBar }
            ]
        });

        effects = TestBed.inject(PoliciesEffects);
        bucketsService = TestBed.inject(BucketsService) as vi.Mocked<BucketsService>;
        errorHelper = TestBed.inject(ErrorHelper) as vi.Mocked<ErrorHelper>;
        snackBar = TestBed.inject(MatSnackBar) as vi.Mocked<MatSnackBar>;
    });

    describe('loadPolicies$', () => {
        it('should return loadPoliciesSuccess with policies on success', () =>
            new Promise<void>((resolve) => {
                const policies = [createPolicy(), createPolicy({ identifier: 'policy-2', action: 'write' })];
                bucketsService.getPolicies.mockReturnValue(of(policies));

                actions$ = of(
                    PoliciesActions.loadPolicies({
                        request: { bucketId: 'bucket-1', context: ErrorContextKey.MANAGE_ACCESS }
                    })
                );

                effects.loadPolicies$.subscribe((action) => {
                    expect(action).toEqual(
                        PoliciesActions.loadPoliciesSuccess({
                            response: {
                                bucketId: 'bucket-1',
                                policies
                            }
                        })
                    );
                    resolve();
                });
            }));

        it('should return error actions on failure', () =>
            new Promise<void>((resolve) => {
                const error = new HttpErrorResponse({ status: 500, statusText: 'Server Error' });
                bucketsService.getPolicies.mockReturnValue(throwError(() => error));
                errorHelper.getErrorString.mockReturnValue('Error loading policies');

                actions$ = of(
                    PoliciesActions.loadPolicies({
                        request: { bucketId: 'bucket-1', context: ErrorContextKey.MANAGE_ACCESS }
                    })
                );

                effects.loadPolicies$.subscribe((action) => {
                    if (action.type === PoliciesActions.loadPoliciesFailure.type) {
                        expect(action).toEqual(PoliciesActions.loadPoliciesFailure());
                        resolve();
                    }
                });
            }));
    });

    describe('loadPolicyTenants$', () => {
        it('should return loadPolicyTenantsSuccess with tenants on success', () =>
            new Promise<void>((resolve) => {
                const users = [createPolicySubject()];
                const userGroups = [
                    createPolicySubject({ identifier: 'group-1', identity: 'test-group', type: 'group' })
                ];
                bucketsService.getBucketPolicyTenants.mockReturnValue(of({ users, userGroups }));

                actions$ = of(
                    PoliciesActions.loadPolicyTenants({ request: { context: ErrorContextKey.MANAGE_ACCESS } })
                );

                effects.loadPolicyTenants$.subscribe((action) => {
                    expect(action).toEqual(
                        PoliciesActions.loadPolicyTenantsSuccess({
                            response: { users, userGroups }
                        })
                    );
                    resolve();
                });
            }));

        it('should return error actions on failure', () =>
            new Promise<void>((resolve) => {
                const error = new HttpErrorResponse({ status: 403, statusText: 'Forbidden' });
                bucketsService.getBucketPolicyTenants.mockReturnValue(throwError(() => error));
                errorHelper.getErrorString.mockReturnValue('Error loading tenants');

                actions$ = of(
                    PoliciesActions.loadPolicyTenants({ request: { context: ErrorContextKey.MANAGE_ACCESS } })
                );

                effects.loadPolicyTenants$.subscribe((action) => {
                    if (action.type === PoliciesActions.loadPolicyTenantsFailure.type) {
                        expect(action).toEqual(PoliciesActions.loadPolicyTenantsFailure());
                        resolve();
                    }
                });
            }));
    });

    describe('saveBucketPolicy$', () => {
        it('should save policy and reload when isLastInBatch is true', () =>
            new Promise<void>((resolve) => {
                const savedPolicy = createPolicy();
                const allPolicies = [savedPolicy];
                bucketsService.saveBucketPolicy.mockReturnValue(of(savedPolicy));
                bucketsService.getPolicies.mockReturnValue(of(allPolicies));

                actions$ = of(
                    PoliciesActions.saveBucketPolicy({
                        request: {
                            bucketId: 'bucket-1',
                            action: 'read',
                            users: [createPolicySubject()],
                            userGroups: [],
                            isLastInBatch: true
                        }
                    })
                );

                let actionCount = 0;
                effects.saveBucketPolicy$.subscribe((action) => {
                    actionCount++;
                    if (action.type === PoliciesActions.loadPoliciesSuccess.type) {
                        expect(action).toEqual(
                            PoliciesActions.loadPoliciesSuccess({
                                response: {
                                    bucketId: 'bucket-1',
                                    policies: allPolicies
                                }
                            })
                        );
                    }
                    if (action.type === PoliciesActions.policyChangeSuccessToast.type) {
                        expect(action).toEqual(
                            PoliciesActions.policyChangeSuccessToast({
                                message: 'Bucket policies saved'
                            })
                        );
                        expect(actionCount).toBe(2);
                        resolve();
                    }
                });
            }));

        it('should save policy without reload when isLastInBatch is false', () =>
            new Promise<void>((resolve) => {
                const savedPolicy = createPolicy();
                bucketsService.saveBucketPolicy.mockReturnValue(of(savedPolicy));

                actions$ = of(
                    PoliciesActions.saveBucketPolicy({
                        request: {
                            bucketId: 'bucket-1',
                            action: 'read',
                            users: [createPolicySubject()],
                            userGroups: [],
                            isLastInBatch: false
                        }
                    })
                );

                effects.saveBucketPolicy$.subscribe((action) => {
                    expect(action).toEqual(PoliciesActions.policiesNoOp());
                    expect(bucketsService.getPolicies).not.toHaveBeenCalled();
                    resolve();
                });
            }));

        it('should return error actions on save failure', () =>
            new Promise<void>((resolve) => {
                const error = new HttpErrorResponse({ status: 409, statusText: 'Conflict' });
                bucketsService.saveBucketPolicy.mockReturnValue(throwError(() => error));
                errorHelper.getErrorString.mockReturnValue('Error saving policy');

                actions$ = of(
                    PoliciesActions.saveBucketPolicy({
                        request: {
                            bucketId: 'bucket-1',
                            action: 'read',
                            users: [],
                            userGroups: []
                        }
                    })
                );

                effects.saveBucketPolicy$.subscribe((action) => {
                    if (action.type === PoliciesActions.saveBucketPolicyFailure.type) {
                        expect(action).toEqual(PoliciesActions.saveBucketPolicyFailure());
                        resolve();
                    }
                });
            }));
    });

    describe('policyChangeSuccessToast$', () => {
        it('should open snackbar with message', () =>
            new Promise<void>((resolve) => {
                actions$ = of(PoliciesActions.policyChangeSuccessToast({ message: 'Success!' }));

                effects.policyChangeSuccessToast$.subscribe(() => {
                    expect(snackBar.open).toHaveBeenCalledWith('Success!', 'Dismiss', { duration: 3000 });
                    resolve();
                });
            }));

        it('should use default message if none provided', () =>
            new Promise<void>((resolve) => {
                actions$ = of(PoliciesActions.policyChangeSuccessToast({ message: '' }));

                effects.policyChangeSuccessToast$.subscribe(() => {
                    expect(snackBar.open).toHaveBeenCalledWith('Policy updated', 'Dismiss', { duration: 3000 });
                    resolve();
                });
            }));
    });
});
