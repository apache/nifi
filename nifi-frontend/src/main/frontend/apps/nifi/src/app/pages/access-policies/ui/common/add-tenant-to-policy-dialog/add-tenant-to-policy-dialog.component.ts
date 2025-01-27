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

import { Component, DestroyRef, EventEmitter, inject, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { AccessPolicy } from '../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { MatListModule } from '@angular/material/list';
import { TenantEntity, UserEntity, UserGroupEntity } from '../../../../../state/shared';
import { AddTenantsToPolicyRequest, AddTenantToPolicyDialogRequest } from '../../../state/access-policy';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'add-tenant-to-policy-dialog',
    imports: [
        MatDialogModule,
        MatButtonModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule,
        MatRadioModule,
        MatCheckboxModule,
        AsyncPipe,
        MatListModule,
        NifiSpinnerDirective
    ],
    templateUrl: './add-tenant-to-policy-dialog.component.html',
    styleUrls: ['./add-tenant-to-policy-dialog.component.scss']
})
export class AddTenantToPolicyDialog extends CloseOnEscapeDialog {
    @Input() set users$(users$: Observable<UserEntity[]>) {
        users$.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((users: UserEntity[]) => {
            const policy: AccessPolicy = this.request.accessPolicy.component;

            this.filteredUsers = users.filter((user: UserEntity) => {
                return !policy.users.some((tenant: TenantEntity) => tenant.id === user.id);
            });

            this.userLookup.clear();
            this.filteredUsers.forEach((user: UserEntity) => {
                this.userLookup.set(user.id, user);
            });
        });
    }

    @Input() set userGroups$(userGroups$: Observable<UserGroupEntity[]>) {
        userGroups$.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((userGroups: UserGroupEntity[]) => {
            const policy: AccessPolicy = this.request.accessPolicy.component;

            this.filteredUserGroups = userGroups.filter((userGroup: UserGroupEntity) => {
                return !policy.userGroups.some((tenant: TenantEntity) => tenant.id === userGroup.id);
            });

            this.userGroupLookup.clear();
            this.filteredUserGroups.forEach((user: UserGroupEntity) => {
                this.userGroupLookup.set(user.id, user);
            });
        });
    }

    @Input() saving$!: Observable<boolean>;

    @Output() addTenants: EventEmitter<AddTenantsToPolicyRequest> = new EventEmitter<AddTenantsToPolicyRequest>();

    private destroyRef = inject(DestroyRef);

    addTenantsForm: FormGroup;
    filteredUsers: UserEntity[] = [];
    filteredUserGroups: UserGroupEntity[] = [];

    userLookup: Map<string, UserEntity> = new Map<string, UserEntity>();
    userGroupLookup: Map<string, UserGroupEntity> = new Map<string, UserGroupEntity>();

    constructor(
        @Inject(MAT_DIALOG_DATA) private request: AddTenantToPolicyDialogRequest,
        private formBuilder: FormBuilder
    ) {
        super();
        this.addTenantsForm = this.formBuilder.group({
            users: new FormControl([]),
            userGroups: new FormControl([])
        });
    }

    addClicked(): void {
        const users: TenantEntity[] = [];

        const usersSelected: string[] = this.addTenantsForm.get('users')?.value;
        usersSelected.forEach((userId: string) => {
            const user: UserEntity | undefined = this.userLookup.get(userId);
            if (user) {
                users.push({
                    id: user.id,
                    revision: user.revision,
                    permissions: user.permissions,
                    component: {
                        id: user.component.id,
                        identity: user.component.identity,
                        configurable: user.component.configurable
                    }
                });
            }
        });

        const userGroups: TenantEntity[] = [];
        const userGroupsSelected: string[] = this.addTenantsForm.get('userGroups')?.value;
        userGroupsSelected.forEach((userGroupId: string) => {
            const userGroup: UserGroupEntity | undefined = this.userGroupLookup.get(userGroupId);
            if (userGroup) {
                userGroups.push({
                    id: userGroup.id,
                    revision: userGroup.revision,
                    permissions: userGroup.permissions,
                    component: {
                        id: userGroup.component.id,
                        identity: userGroup.component.identity,
                        configurable: userGroup.component.configurable
                    }
                });
            }
        });

        this.addTenants.next({
            users,
            userGroups
        });
    }

    override isDirty(): boolean {
        return this.addTenantsForm.dirty;
    }
}
