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

import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Store } from '@ngrx/store';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import {
    createAccessPolicy,
    openAddTenantToPolicyDialog,
    promptDeleteAccessPolicy,
    promptOverrideAccessPolicy,
    promptRemoveTenantFromPolicy,
    reloadAccessPolicy,
    resetAccessPolicyState,
    selectGlobalAccessPolicy,
    setAccessPolicy
} from '../../state/access-policy/access-policy.actions';
import { AccessPolicyState, RemoveTenantFromPolicyRequest } from '../../state/access-policy';
import { initialState } from '../../state/access-policy/access-policy.reducer';
import {
    selectAccessPolicyState,
    selectGlobalResourceActionFromRoute
} from '../../state/access-policy/access-policy.selectors';
import { distinctUntilChanged } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { ComponentType, isDefinedAndNotNull, NiFiCommon, SelectOption, TextTip } from '@nifi/shared';
import { RequiredPermission } from '../../../../state/shared';
import { AccessPolicyEntity, Action, PolicyStatus } from '../../state/shared';
import { loadExtensionTypesForPolicies } from '../../../../state/extension-types/extension-types.actions';
import { selectRequiredPermissions } from '../../../../state/extension-types/extension-types.selectors';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { AccessPoliciesState } from '../../state';
import { loadTenants, resetTenantsState } from '../../state/tenants/tenants.actions';
import { loadCurrentUser } from '../../../../state/current-user/current-user.actions';
import { ErrorContextKey } from '../../../../state/error';

@Component({
    selector: 'global-access-policies',
    templateUrl: './global-access-policies.component.html',
    styleUrls: ['./global-access-policies.component.scss'],
    standalone: false
})
export class GlobalAccessPolicies implements OnInit, OnDestroy {
    flowConfiguration$ = this.store.select(selectFlowConfiguration);
    accessPolicyState$ = this.store.select(selectAccessPolicyState);
    currentUser$ = this.store.select(selectCurrentUser);

    protected readonly TextTip = TextTip;
    protected readonly Action = Action;
    protected readonly PolicyStatus = PolicyStatus;
    protected readonly ComponentType = ComponentType;

    policyForm: FormGroup;
    resourceOptions: SelectOption[];
    requiredPermissionOptions!: SelectOption[];
    supportsReadWriteAction = false;
    supportsResourceIdentifier = false;

    @ViewChild('inheritedFromPolicies') inheritedFromPolicies!: TemplateRef<any>;
    @ViewChild('inheritedFromController') inheritedFromController!: TemplateRef<any>;
    @ViewChild('inheritedFromNoRestrictions') inheritedFromNoRestrictions!: TemplateRef<any>;

    constructor(
        private store: Store<AccessPoliciesState>,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.resourceOptions = this.nifiCommon.getAllPolicyTypeListing();

        this.policyForm = this.formBuilder.group({
            resource: new FormControl(null, Validators.required),
            action: new FormControl(null, Validators.required)
        });

        this.store
            .select(selectRequiredPermissions)
            .pipe(takeUntilDestroyed())
            .subscribe((requiredPermissions: RequiredPermission[]) => {
                const regardlessOfRestrictions = 'regardless of restrictions';

                const options: SelectOption[] = [
                    {
                        text: regardlessOfRestrictions,
                        value: '',
                        description:
                            'Allows users to create/modify all restricted components regardless of restrictions.'
                    }
                ];

                options.push(
                    ...requiredPermissions.map((requiredPermission) => ({
                        text: "requiring '" + requiredPermission.label + "'",
                        value: requiredPermission.id,
                        description:
                            "Allows users to create/modify restricted components requiring '" +
                            requiredPermission.label +
                            "'"
                    }))
                );

                this.requiredPermissionOptions = options.sort((a: SelectOption, b: SelectOption): number => {
                    if (a.text === regardlessOfRestrictions) {
                        return -1;
                    } else if (b.text === regardlessOfRestrictions) {
                        return 1;
                    }

                    return this.nifiCommon.compareString(a.text, b.text);
                });
            });

        this.store
            .select(selectGlobalResourceActionFromRoute)
            .pipe(
                isDefinedAndNotNull(),
                distinctUntilChanged((a, b) => {
                    return (
                        a.action == b.action && a.resource == b.resource && a.resourceIdentifier == b.resourceIdentifier
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe((resourceAction) => {
                if (resourceAction) {
                    this.supportsReadWriteAction = this.globalPolicySupportsReadWrite(resourceAction.resource);

                    this.policyForm.get('resource')?.setValue(resourceAction.resource);
                    this.policyForm.get('action')?.setValue(resourceAction.action);

                    this.updateResourceIdentifierVisibility(resourceAction.resource);

                    if (resourceAction.resource === 'restricted-components' && resourceAction.resourceIdentifier) {
                        this.policyForm.get('resourceIdentifier')?.setValue(resourceAction.resourceIdentifier);
                    }

                    this.store.dispatch(
                        setAccessPolicy({
                            request: {
                                resourceAction
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadTenants());
        this.store.dispatch(loadExtensionTypesForPolicies());
    }

    isInitialLoading(state: AccessPolicyState): boolean {
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    resourceChanged(value: string): void {
        if (this.globalPolicySupportsReadWrite(value)) {
            this.supportsReadWriteAction = true;
            this.supportsResourceIdentifier = false;

            // reset the action
            this.policyForm.get('action')?.setValue(Action.Read);
        } else {
            this.supportsReadWriteAction = false;

            // since this resource does not support read and write, update the form with the appropriate action this resource does support
            this.policyForm.get('action')?.setValue(this.globalPolicySupportsWrite(value) ? Action.Write : Action.Read);

            this.updateResourceIdentifierVisibility(value);
        }

        this.store.dispatch(
            selectGlobalAccessPolicy({
                request: {
                    resourceAction: {
                        resource: this.policyForm.get('resource')?.value,
                        action: this.policyForm.get('action')?.value,
                        resourceIdentifier: this.policyForm.get('resourceIdentifier')?.value
                    }
                }
            })
        );
    }

    private globalPolicySupportsReadWrite(resource: string): boolean {
        return (
            resource === 'controller' ||
            resource === 'parameter-contexts' ||
            resource === 'counters' ||
            resource === 'policies' ||
            resource === 'tenants'
        );
    }

    private globalPolicySupportsWrite(resource: string): boolean {
        return resource === 'proxy' || resource === 'restricted-components';
    }

    private updateResourceIdentifierVisibility(resource: string): void {
        if (resource === 'restricted-components') {
            this.supportsResourceIdentifier = true;
            this.policyForm.addControl('resourceIdentifier', new FormControl(''));
        } else {
            this.supportsResourceIdentifier = false;
            this.policyForm.removeControl('resourceIdentifier');
        }
    }

    actionChanged(): void {
        this.store.dispatch(
            selectGlobalAccessPolicy({
                request: {
                    resourceAction: {
                        resource: this.policyForm.get('resource')?.value,
                        action: this.policyForm.get('action')?.value,
                        resourceIdentifier: this.policyForm.get('resourceIdentifier')?.value
                    }
                }
            })
        );
    }

    resourceIdentifierChanged(): void {
        this.store.dispatch(
            selectGlobalAccessPolicy({
                request: {
                    resourceAction: {
                        resource: this.policyForm.get('resource')?.value,
                        action: this.policyForm.get('action')?.value,
                        resourceIdentifier: this.policyForm.get('resourceIdentifier')?.value
                    }
                }
            })
        );
    }

    getTemplateForInheritedPolicy(policy: AccessPolicyEntity): TemplateRef<any> {
        if (policy.component.resource === '/policies') {
            return this.inheritedFromPolicies;
        } else if (policy.component.resource === '/controller') {
            return this.inheritedFromController;
        }

        return this.inheritedFromNoRestrictions;
    }

    createNewPolicy(): void {
        this.store.dispatch(createAccessPolicy());
    }

    overridePolicy(): void {
        this.store.dispatch(promptOverrideAccessPolicy());
    }

    removeTenantFromPolicy(request: RemoveTenantFromPolicyRequest): void {
        this.store.dispatch(
            promptRemoveTenantFromPolicy({
                request
            })
        );
    }

    addTenantToPolicy(): void {
        this.store.dispatch(openAddTenantToPolicyDialog());
    }

    deletePolicy(): void {
        this.store.dispatch(promptDeleteAccessPolicy());
    }

    refreshGlobalAccessPolicy(): void {
        this.store.dispatch(reloadAccessPolicy());
    }

    ngOnDestroy(): void {
        // reload the current user to ensure the latest global policies
        this.store.dispatch(loadCurrentUser());

        this.store.dispatch(resetAccessPolicyState());
        this.store.dispatch(resetTenantsState());
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
