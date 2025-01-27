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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { EditTenantRequest, EditTenantResponse, UserEntity, UserGroupEntity } from '../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    FormsModule,
    ReactiveFormsModule,
    ValidationErrors,
    ValidatorFn,
    Validators
} from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { NifiSpinnerDirective } from '../spinner/nifi-spinner.directive';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { MatListModule } from '@angular/material/list';
import { Client } from '../../../service/client.service';
import { NiFiCommon, CloseOnEscapeDialog, Revision } from '@nifi/shared';
import { ErrorContextKey } from '../../../state/error';
import { ContextErrorBanner } from '../context-error-banner/context-error-banner.component';

@Component({
    selector: 'edit-tenant-dialog',
    imports: [
        MatDialogModule,
        MatButtonModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule,
        MatRadioModule,
        MatCheckboxModule,
        NifiSpinnerDirective,
        AsyncPipe,
        MatListModule,
        ContextErrorBanner
    ],
    templateUrl: './edit-tenant-dialog.component.html',
    styleUrls: ['./edit-tenant-dialog.component.scss']
})
export class EditTenantDialog extends CloseOnEscapeDialog {
    @Input() saving$!: Observable<boolean>;
    @Output() editTenant: EventEmitter<EditTenantResponse> = new EventEmitter<EditTenantResponse>();
    @Output() close: EventEmitter<void> = new EventEmitter<void>();

    readonly USER: string = 'user';
    readonly USER_GROUP: string = 'userGroup';
    isUser = true;

    identity: FormControl;
    tenantType: FormControl;
    editTenantForm: FormGroup;
    isNew: boolean;

    users: UserEntity[];
    userGroups: UserGroupEntity[];

    constructor(
        @Inject(MAT_DIALOG_DATA) private request: EditTenantRequest,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon,
        private client: Client
    ) {
        super();
        const user: UserEntity | undefined = request.user;
        const userGroup: UserGroupEntity | undefined = request.userGroup;

        if (user || userGroup) {
            this.isNew = false;

            let identity = '';
            let tenantType: string = this.USER;
            if (user) {
                identity = user.component.identity;
            } else if (userGroup) {
                identity = userGroup.component.identity;
                tenantType = this.USER_GROUP;
            }
            this.identity = new FormControl(identity, [
                Validators.required,
                this.existingTenantValidator(request.existingUsers, request.existingUserGroups, identity)
            ]);
            this.tenantType = new FormControl({ value: tenantType, disabled: true });
        } else {
            this.isNew = true;

            this.identity = new FormControl('', [
                Validators.required,
                this.existingTenantValidator(request.existingUsers, request.existingUserGroups)
            ]);
            this.tenantType = new FormControl(this.USER);
        }

        this.users = request.existingUsers.slice().sort((a: UserEntity, b: UserEntity) => {
            return this.nifiCommon.compareString(a.component.identity, b.component.identity);
        });
        this.userGroups = request.existingUserGroups.slice().sort((a: UserGroupEntity, b: UserGroupEntity) => {
            return this.nifiCommon.compareString(a.component.identity, b.component.identity);
        });

        this.editTenantForm = this.formBuilder.group({
            identity: this.identity,
            tenantType: this.tenantType
        });

        this.tenantTypeChanged();
    }

    private existingTenantValidator(
        existingUsers: UserEntity[],
        existingUserGroups: UserGroupEntity[],
        currentIdentity?: string
    ): ValidatorFn {
        const existingUserNames: string[] = existingUsers.map((user) => user.component.identity);
        const existingUserGroupNames: string[] = existingUserGroups.map((userGroup) => userGroup.component.identity);

        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value === '') {
                return null;
            }

            const existingTenants: string[] = this.isUser ? existingUserNames : existingUserGroupNames;
            if (existingTenants.includes(value) && value != currentIdentity) {
                return {
                    existingTenant: true
                };
            }
            return null;
        };
    }

    getIdentityErrorMessage(): string {
        if (this.identity.hasError('required')) {
            return 'Identity is required.';
        }

        const tenantType: string = this.isUser ? 'user' : 'user group';
        return this.identity.hasError('existingTenant') ? `A ${tenantType} with this identity already exists.` : '';
    }

    tenantTypeChanged(): void {
        this.isUser = this.editTenantForm.get('tenantType')?.value == this.USER;
        if (this.isUser) {
            this.setupFormWithExistingUserGroups();
        } else {
            this.setupFormWithExistingUsers();
        }
    }

    setupFormWithExistingUsers(): void {
        if (this.editTenantForm.contains('userGroups')) {
            this.editTenantForm.removeControl('userGroups');
        }

        const users: string[] = [];
        if (this.request.userGroup) {
            users.push(...this.request.userGroup.component.users.map((user) => user.id));
        }

        this.editTenantForm.addControl('users', new FormControl(users));
    }

    setupFormWithExistingUserGroups(): void {
        if (this.editTenantForm.contains('users')) {
            this.editTenantForm.removeControl('users');
        }

        const userGroups: string[] = [];
        if (this.request.user) {
            userGroups.push(...this.request.user.component.userGroups.map((userGroup) => userGroup.id));
        }

        this.editTenantForm.addControl('userGroups', new FormControl(userGroups));
    }

    cancelClicked(): void {
        this.close.next();
    }

    okClicked(): void {
        if (this.isUser) {
            const revision: Revision = this.isNew
                ? {
                      clientId: this.client.getClientId(),
                      version: 0
                  }
                : this.client.getRevision(this.request.user);

            const userGroupsAdded: string[] = [];
            const userGroupsRemoved: string[] = [];
            const userGroupsSelected: string[] = this.editTenantForm.get('userGroups')?.value;
            if (this.request.user) {
                const userGroups: string[] = this.request.user.component.userGroups.map((userGroup) => userGroup.id);

                userGroupsAdded.push(...userGroupsSelected.filter((x) => !userGroups.includes(x)));
                userGroupsRemoved.push(...userGroups.filter((x) => !userGroupsSelected.includes(x)));
            } else {
                userGroupsAdded.push(...userGroupsSelected);
            }

            this.editTenant.next({
                revision,
                user: {
                    payload: {
                        identity: this.editTenantForm.get('identity')?.value
                    },
                    userGroupsAdded,
                    userGroupsRemoved
                }
            });
        } else {
            const revision: Revision = this.isNew
                ? {
                      clientId: this.client.getClientId(),
                      version: 0
                  }
                : this.client.getRevision(this.request.userGroup);

            const users: string[] = this.editTenantForm.get('users')?.value;

            this.editTenant.next({
                revision,
                userGroup: {
                    payload: {
                        identity: this.editTenantForm.get('identity')?.value
                    },
                    users
                }
            });
        }
    }

    override isDirty(): boolean {
        return this.editTenantForm.dirty;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
