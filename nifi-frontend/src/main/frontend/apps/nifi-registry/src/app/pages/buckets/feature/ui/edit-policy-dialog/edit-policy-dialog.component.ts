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

import { Component, OnInit, inject } from '@angular/core';
import { MatDialogModule, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { FormsModule } from '@angular/forms';
import { PolicySection } from 'apps/nifi-registry/src/app/state/policies';

export interface EditPolicyDialogData {
    identity: string;
    type: 'user' | 'group';
    currentPermissions: PolicySection[];
}

export interface EditPolicyResult {
    permissions: PolicySection[];
}

@Component({
    selector: 'edit-policy-dialog',
    templateUrl: './edit-policy-dialog.component.html',
    styleUrl: './edit-policy-dialog.component.scss',
    standalone: true,
    imports: [MatDialogModule, MatButtonModule, MatCheckboxModule, MatFormFieldModule, MatInputModule, FormsModule]
})
export class EditPolicyDialogComponent implements OnInit {
    protected data = inject<EditPolicyDialogData>(MAT_DIALOG_DATA);
    private dialogRef = inject(MatDialogRef<EditPolicyDialogComponent>);

    // Permissions
    readChecked = false;
    writeChecked = false;
    deleteChecked = false;

    get allChecked(): boolean {
        return this.readChecked && this.writeChecked && this.deleteChecked;
    }

    get canApply(): boolean {
        return this.readChecked || this.writeChecked || this.deleteChecked;
    }

    ngOnInit(): void {
        // Initialize checkboxes based on current permissions
        this.readChecked = this.data.currentPermissions.includes('read');
        this.writeChecked = this.data.currentPermissions.includes('write');
        this.deleteChecked = this.data.currentPermissions.includes('delete');
    }

    toggleAll(checked: boolean): void {
        this.readChecked = checked;
        this.writeChecked = checked;
        this.deleteChecked = checked;
    }

    apply(): void {
        const permissions: PolicySection[] = [];
        if (this.readChecked) permissions.push('read');
        if (this.writeChecked) permissions.push('write');
        if (this.deleteChecked) permissions.push('delete');

        const result: EditPolicyResult = {
            permissions
        };

        this.dialogRef.close(result);
    }

    cancel(): void {
        this.dialogRef.close();
    }
}
