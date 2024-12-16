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

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { Router } from '@angular/router';
import * as AccessPolicyActions from './access-policy.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { catchError, from, map, of, switchMap, take, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { AccessPolicyService } from '../../service/access-policy.service';
import { AccessPolicyEntity, ComponentResourceAction, PolicyStatus, ResourceAction } from '../shared';
import { selectAccessPolicy, selectResourceAction, selectSaving } from './access-policy.selectors';
import { isDefinedAndNotNull, MEDIUM_DIALOG, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import { TenantEntity } from '../../../../state/shared';
import { AddTenantToPolicyDialog } from '../../ui/common/add-tenant-to-policy-dialog/add-tenant-to-policy-dialog.component';
import { AddTenantsToPolicyRequest } from './index';
import { selectUserGroups, selectUsers } from '../tenants/tenants.selectors';
import { OverridePolicyDialog } from '../../ui/common/override-policy-dialog/override-policy-dialog.component';
import { HttpErrorResponse } from '@angular/common/http';
import { loadCurrentUser } from '../../../../state/current-user/current-user.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class AccessPolicyEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private router: Router,
        private accessPoliciesService: AccessPolicyService,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper
    ) {}

    setAccessPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.setAccessPolicy),
            map((action) => action.request),
            switchMap((request) =>
                of(
                    AccessPolicyActions.loadAccessPolicy({
                        request: {
                            resourceAction: request.resourceAction
                        }
                    })
                )
            )
        )
    );

    reloadAccessPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.reloadAccessPolicy),
            concatLatestFrom(() => this.store.select(selectResourceAction).pipe(isDefinedAndNotNull())),
            switchMap(([, resourceAction]) => {
                return of(
                    AccessPolicyActions.loadAccessPolicy({
                        request: {
                            resourceAction
                        }
                    })
                );
            })
        )
    );

    loadAccessPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.loadAccessPolicy),
            map((action) => action.request),
            switchMap((request) =>
                from(this.accessPoliciesService.getAccessPolicy(request.resourceAction)).pipe(
                    map((response) => {
                        const accessPolicy: AccessPolicyEntity = response;

                        let requestedResource = `/${request.resourceAction.resource}`;
                        if (request.resourceAction.resourceIdentifier) {
                            requestedResource += `/${request.resourceAction.resourceIdentifier}`;
                        }

                        let policyStatus: PolicyStatus | undefined;
                        if (accessPolicy.component.resource === requestedResource) {
                            policyStatus = PolicyStatus.Found;
                        } else {
                            policyStatus = PolicyStatus.Inherited;
                        }

                        return AccessPolicyActions.loadAccessPolicySuccess({
                            response: {
                                accessPolicy,
                                policyStatus
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        let policyStatus: PolicyStatus | undefined;
                        if (errorResponse.status === 404) {
                            policyStatus = PolicyStatus.NotFound;
                        } else if (errorResponse.status === 403) {
                            policyStatus = PolicyStatus.Forbidden;
                        }

                        if (policyStatus) {
                            return of(
                                AccessPolicyActions.resetAccessPolicy({
                                    response: {
                                        policyStatus
                                    }
                                })
                            );
                        } else {
                            return of(
                                AccessPolicyActions.accessPolicyApiBannerError({
                                    response: {
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    }
                                })
                            );
                        }
                    })
                )
            )
        )
    );

    createAccessPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.createAccessPolicy),
            concatLatestFrom(() => this.store.select(selectResourceAction).pipe(isDefinedAndNotNull())),
            switchMap(([, resourceAction]) =>
                from(this.accessPoliciesService.createAccessPolicy(resourceAction)).pipe(
                    map((response) => {
                        // reload the current user to reflect the latest permission changes
                        this.store.dispatch(loadCurrentUser());

                        const accessPolicy: AccessPolicyEntity = response;
                        const policyStatus: PolicyStatus = PolicyStatus.Found;

                        return AccessPolicyActions.createAccessPolicySuccess({
                            response: {
                                accessPolicy,
                                policyStatus
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessPolicyActions.accessPolicyApiBannerError({
                                response: {
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    promptOverrideAccessPolicy$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessPolicyActions.promptOverrideAccessPolicy),
                concatLatestFrom(() => this.store.select(selectAccessPolicy).pipe(isDefinedAndNotNull())),
                tap(([, accessPolicy]) => {
                    const dialogReference = this.dialog.open(OverridePolicyDialog, {
                        ...SMALL_DIALOG
                    });

                    dialogReference.componentInstance.copyInheritedPolicy
                        .pipe(take(1))
                        .subscribe((copyInheritedPolicy: boolean) => {
                            dialogReference.close();

                            const users: TenantEntity[] = [];
                            const userGroups: TenantEntity[] = [];

                            if (copyInheritedPolicy) {
                                users.push(...accessPolicy.component.users);
                                userGroups.push(...accessPolicy.component.userGroups);
                            }

                            this.store.dispatch(
                                AccessPolicyActions.overrideAccessPolicy({
                                    request: {
                                        users,
                                        userGroups
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    overrideAccessPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.overrideAccessPolicy),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectResourceAction).pipe(isDefinedAndNotNull())),
            switchMap(([request, resourceAction]) =>
                from(
                    this.accessPoliciesService.createAccessPolicy(resourceAction, {
                        userGroups: request.userGroups,
                        users: request.users
                    })
                ).pipe(
                    map((response) => {
                        // reload the current user to reflect the latest permission changes
                        this.store.dispatch(loadCurrentUser());

                        const accessPolicy: AccessPolicyEntity = response;
                        const policyStatus: PolicyStatus = PolicyStatus.Found;

                        return AccessPolicyActions.createAccessPolicySuccess({
                            response: {
                                accessPolicy,
                                policyStatus
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessPolicyActions.accessPolicyApiBannerError({
                                response: {
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    selectGlobalAccessPolicy$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessPolicyActions.selectGlobalAccessPolicy),
                map((action) => action.request),
                tap((request) => {
                    const resourceAction: ResourceAction = request.resourceAction;
                    const commands: string[] = [
                        '/access-policies',
                        'global',
                        resourceAction.action,
                        resourceAction.resource
                    ];
                    if (resourceAction.resourceIdentifier) {
                        commands.push(resourceAction.resourceIdentifier);
                    }

                    this.router.navigate(commands);
                })
            ),
        { dispatch: false }
    );

    selectComponentAccessPolicy$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessPolicyActions.selectComponentAccessPolicy),
                map((action) => action.request),
                tap((request) => {
                    const resourceAction: ComponentResourceAction = request.resourceAction;
                    const commands: string[] = [
                        '/access-policies',
                        resourceAction.action,
                        resourceAction.policy,
                        resourceAction.resource,
                        resourceAction.resourceIdentifier
                    ];
                    this.router.navigate(commands);
                })
            ),
        { dispatch: false }
    );

    openAddTenantToPolicyDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessPolicyActions.openAddTenantToPolicyDialog),
                concatLatestFrom(() => this.store.select(selectAccessPolicy)),
                tap(([, accessPolicy]) => {
                    const dialogReference = this.dialog.open(AddTenantToPolicyDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            accessPolicy
                        }
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);
                    dialogReference.componentInstance.users$ = this.store.select(selectUsers);
                    dialogReference.componentInstance.userGroups$ = this.store.select(selectUserGroups);

                    dialogReference.componentInstance.addTenants
                        .pipe(take(1))
                        .subscribe((request: AddTenantsToPolicyRequest) => {
                            this.store.dispatch(AccessPolicyActions.addTenantsToPolicy({ request }));
                        });
                })
            ),
        { dispatch: false }
    );

    addTenantsToPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.addTenantsToPolicy),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectAccessPolicy).pipe(isDefinedAndNotNull())),
            switchMap(([request, accessPolicy]) => {
                const users: TenantEntity[] = [...accessPolicy.component.users, ...request.users];
                const userGroups: TenantEntity[] = [...accessPolicy.component.userGroups, ...request.userGroups];

                return from(this.accessPoliciesService.updateAccessPolicy(accessPolicy, users, userGroups)).pipe(
                    map((response: any) => {
                        this.dialog.closeAll();

                        // reload the current user to reflect the latest permission changes
                        this.store.dispatch(loadCurrentUser());

                        return AccessPolicyActions.loadAccessPolicySuccess({
                            response: {
                                accessPolicy: response,
                                policyStatus: PolicyStatus.Found
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessPolicyActions.accessPolicyApiBannerError({
                                response: {
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                );
            })
        )
    );

    promptRemoveTenantFromPolicy$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessPolicyActions.promptRemoveTenantFromPolicy),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Update Policy',
                            message: `Remove '${request.tenant.component.identity}' from this policy?`
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(AccessPolicyActions.removeTenantFromPolicy({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    removeTenantFromPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.removeTenantFromPolicy),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectAccessPolicy).pipe(isDefinedAndNotNull())),
            switchMap(([request, accessPolicy]) => {
                const users: TenantEntity[] = [...accessPolicy.component.users];
                const userGroups: TenantEntity[] = [...accessPolicy.component.userGroups];

                let tenants: TenantEntity[];
                if (request.tenantType === 'user') {
                    tenants = users;
                } else {
                    tenants = userGroups;
                }

                if (tenants) {
                    const tenantIndex: number = tenants.findIndex(
                        (tenant: TenantEntity) => request.tenant.id === tenant.id
                    );
                    if (tenantIndex > -1) {
                        tenants.splice(tenantIndex, 1);
                    }
                }

                return from(this.accessPoliciesService.updateAccessPolicy(accessPolicy, users, userGroups)).pipe(
                    map((response: any) => {
                        // reload the current user to reflect the latest permission changes
                        this.store.dispatch(loadCurrentUser());

                        return AccessPolicyActions.loadAccessPolicySuccess({
                            response: {
                                accessPolicy: response,
                                policyStatus: PolicyStatus.Found
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessPolicyActions.accessPolicyApiBannerError({
                                response: {
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                );
            })
        )
    );

    promptDeleteAccessPolicy$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(AccessPolicyActions.promptDeleteAccessPolicy),
                tap(() => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Delete Policy',
                            message:
                                'Are you sure you want to delete this policy? By doing so, the permissions for this component will revert to the inherited policy if applicable.'
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(AccessPolicyActions.deleteAccessPolicy());
                    });
                })
            ),
        { dispatch: false }
    );

    deleteAccessPolicy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.deleteAccessPolicy),
            concatLatestFrom(() => [
                this.store.select(selectResourceAction).pipe(isDefinedAndNotNull()),
                this.store.select(selectAccessPolicy).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, resourceAction, accessPolicy]) =>
                from(this.accessPoliciesService.deleteAccessPolicy(accessPolicy)).pipe(
                    map(() => {
                        // reload the current user to reflect the latest permission changes
                        this.store.dispatch(loadCurrentUser());

                        // the policy was removed, we need to reload the policy for this resource and action to fetch
                        // the inherited policy or correctly when it's not found
                        return AccessPolicyActions.loadAccessPolicy({
                            request: {
                                resourceAction
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            AccessPolicyActions.accessPolicyApiBannerError({
                                response: {
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    accessPolicyApiBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(AccessPolicyActions.accessPolicyApiBannerError),
            map((action) => action.response),
            tap(() => this.dialog.closeAll()),
            switchMap((response) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: {
                            errors: [response.error],
                            context: ErrorContextKey.ACCESS_POLICIES
                        }
                    })
                )
            )
        )
    );
}
