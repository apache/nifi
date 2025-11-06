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
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { FormsModule } from '@angular/forms';
import { NiFiCommon } from '@nifi/shared';
import { PolicySubject } from 'apps/nifi-registry/src/app/service/buckets.service';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { PolicySection } from 'apps/nifi-registry/src/app/state/policies';

export interface AddPolicyToBucketDialogData {
    bucket: Bucket;
    existingUsers: string[];
    existingGroups: string[];
    availableUsers: PolicySubject[];
    availableGroups: PolicySubject[];
}

export interface UserOrGroupRow {
    identity: string;
    identifier: string;
    type: 'user' | 'group';
    selected: boolean;
}

export interface AddPolicyResult {
    userOrGroup: PolicySubject;
    permissions: PolicySection[];
}

@Component({
    selector: 'add-policy-to-bucket-dialog',
    templateUrl: './add-policy-to-bucket-dialog.component.html',
    styleUrl: './add-policy-to-bucket-dialog.component.scss',
    standalone: true,
    imports: [MatDialogModule, MatButtonModule, MatCheckboxModule, MatTableModule, MatSortModule, FormsModule]
})
export class AddPolicyToBucketDialogComponent implements OnInit {
    protected data = inject<AddPolicyToBucketDialogData>(MAT_DIALOG_DATA);
    private dialogRef = inject(MatDialogRef<AddPolicyToBucketDialogComponent>);
    private nifiCommon = inject(NiFiCommon);

    dataSource: MatTableDataSource<UserOrGroupRow> = new MatTableDataSource<UserOrGroupRow>();
    displayedColumns: string[] = ['type', 'identity'];
    sort: Sort = {
        active: 'identity',
        direction: 'asc'
    };

    selectedRow: UserOrGroupRow | null = null;

    // Permissions
    readChecked = false;
    writeChecked = false;
    deleteChecked = false;

    get allChecked(): boolean {
        return this.readChecked && this.writeChecked && this.deleteChecked;
    }

    get canApply(): boolean {
        return !!this.selectedRow && (this.readChecked || this.writeChecked || this.deleteChecked);
    }

    ngOnInit(): void {
        // Filter out users and groups that already have policies as users can only create or edit a single policy for a user or group per bucket
        const availableUsers = this.data.availableUsers
            .filter((user) => !this.data.existingUsers.includes(user.identity))
            .map((user) => ({
                identity: user.identity,
                identifier: user.identifier,
                type: 'user' as const,
                selected: false
            }));

        const availableGroups = this.data.availableGroups
            .filter((group) => !this.data.existingGroups.includes(group.identity))
            .map((group) => ({
                identity: group.identity,
                identifier: group.identifier,
                type: 'group' as const,
                selected: false
            }));

        const allRows = [...availableGroups, ...availableUsers];
        this.dataSource.data = this.sortRows(allRows, this.sort);
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortRows(this.dataSource.data, sort);
    }

    sortRows(data: UserOrGroupRow[], sort: Sort): UserOrGroupRow[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'identity':
                    retVal = this.nifiCommon.compareString(a.identity, b.identity);
                    break;
                case 'type':
                    retVal = this.nifiCommon.compareString(a.type, b.type);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    selectRow(row: UserOrGroupRow): void {
        // Deselect all rows
        this.dataSource.data.forEach((r) => (r.selected = false));
        // Select clicked row
        row.selected = true;
        this.selectedRow = row;
    }

    isSelected(row: UserOrGroupRow): boolean {
        return row.selected;
    }

    toggleAll(checked: boolean): void {
        this.readChecked = checked;
        this.writeChecked = checked;
        this.deleteChecked = checked;
    }

    apply(): void {
        if (!this.selectedRow) {
            return;
        }

        const permissions: PolicySection[] = [];
        if (this.readChecked) permissions.push('read');
        if (this.writeChecked) permissions.push('write');
        if (this.deleteChecked) permissions.push('delete');

        const result: AddPolicyResult = {
            userOrGroup: {
                identifier: this.selectedRow.identifier,
                identity: this.selectedRow.identity,
                type: this.selectedRow.type
            },
            permissions
        };

        this.dialogRef.close(result);
    }

    cancel(): void {
        this.dialogRef.close();
    }
}
