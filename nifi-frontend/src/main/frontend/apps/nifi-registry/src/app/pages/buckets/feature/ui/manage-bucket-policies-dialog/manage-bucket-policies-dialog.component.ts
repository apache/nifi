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

import { Component, DestroyRef, OnInit, inject, Output, EventEmitter } from '@angular/core';
import { MatDialogModule, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatMenuModule } from '@angular/material/menu';
import { MatDialog } from '@angular/material/dialog';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { take } from 'rxjs/operators';
import { NiFiCommon, SMALL_DIALOG, YesNoDialog, MEDIUM_DIALOG, LARGE_DIALOG } from '@nifi/shared';
import {
    AddPolicyToBucketDialogComponent,
    AddPolicyResult
} from '../add-policy-to-bucket-dialog/add-policy-to-bucket-dialog.component';
import { EditPolicyDialogComponent, EditPolicyResult } from '../edit-policy-dialog/edit-policy-dialog.component';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { BucketPolicyOptionsView, PolicySelection, PolicySection } from 'apps/nifi-registry/src/app/state/policies';
import { PolicySubject, PolicyRevision } from 'apps/nifi-registry/src/app/service/buckets.service';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../../state/error';

export interface PolicyTableRow {
    identity: string;
    type: 'user' | 'group';
    permissions: string;
    identifier: string;
    actions: PolicySection[];
}

export interface SaveBucketPoliciesRequest {
    bucketId: string;
    action: PolicySection;
    policyId?: string;
    revision?: PolicyRevision;
    users: PolicySubject[];
    userGroups: PolicySubject[];
    isLastInBatch?: boolean;
}

export interface ManageBucketPoliciesDialogData {
    bucket: Bucket;
    options$: Observable<BucketPolicyOptionsView>;
    selection$: Observable<Partial<Record<PolicySection, PolicySelection>>>;
    loading$: Observable<boolean>;
    saving$: Observable<boolean>;
    isAddPolicyDisabled$: Observable<boolean>;
    isPolicyError$: Observable<boolean>;
}

@Component({
    selector: 'manage-bucket-policies-dialog',
    templateUrl: './manage-bucket-policies-dialog.component.html',
    styleUrl: './manage-bucket-policies-dialog.component.scss',
    standalone: true,
    imports: [
        MatDialogModule,
        MatButtonModule,
        MatProgressSpinnerModule,
        MatTableModule,
        MatSortModule,
        MatMenuModule,
        AsyncPipe,
        ContextErrorBanner
    ]
})
export class ManageBucketPoliciesDialogComponent implements OnInit {
    protected data = inject<ManageBucketPoliciesDialogData>(MAT_DIALOG_DATA);
    private dialogRef = inject(MatDialogRef<ManageBucketPoliciesDialogComponent>);
    private destroyRef = inject(DestroyRef);
    private nifiCommon = inject(NiFiCommon);
    private dialog = inject(MatDialog);

    // Get observables from dialog data
    options$ = this.data.options$;
    selection$ = this.data.selection$;
    loading$ = this.data.loading$;
    saving$ = this.data.saving$;
    isPolicyError$ = this.data.isPolicyError$;
    isAddPolicyDisabled$ = this.data.isAddPolicyDisabled$;

    @Output() savePolicies = new EventEmitter<SaveBucketPoliciesRequest>();

    private latestOptions: BucketPolicyOptionsView | null = null;
    private latestSelection: Partial<Record<PolicySection, PolicySelection>> = {};

    dataSource: MatTableDataSource<PolicyTableRow> = new MatTableDataSource<PolicyTableRow>();
    displayedColumns: string[] = ['type', 'identity', 'permissions', 'actions'];
    sort: Sort = {
        active: 'identity',
        direction: 'asc'
    };

    ngOnInit(): void {
        // Subscribe to options$ to keep latest options
        this.options$.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((options) => {
            this.latestOptions = options;
        });

        // Subscribe to selection$ to build table data
        this.selection$.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((selectionMap) => {
            this.latestSelection = selectionMap || {};
            const rows = this.buildTableRows(selectionMap || {});
            this.dataSource.data = this.sortPolicies(rows, this.sort);
        });
    }

    private buildTableRows(selectionMap: Partial<Record<PolicySection, PolicySelection>>): PolicyTableRow[] {
        const rowMap = new Map<string, PolicyTableRow>();

        // Iterate through each policy type (read, write, delete)
        (['read', 'write', 'delete'] as PolicySection[]).forEach((section) => {
            const selection = selectionMap[section];
            if (!selection) return;

            // Process users
            selection.users.forEach((user) => {
                const key = `user-${user.identifier}`;
                const existing = rowMap.get(key);
                if (existing) {
                    existing.actions.push(section);
                    existing.permissions = this.formatPermissions(existing.actions);
                } else {
                    rowMap.set(key, {
                        identity: user.identity,
                        type: 'user',
                        permissions: section,
                        identifier: user.identifier,
                        actions: [section]
                    });
                }
            });

            // Process groups
            selection.userGroups.forEach((group) => {
                const key = `group-${group.identifier}`;
                const existing = rowMap.get(key);
                if (existing) {
                    existing.actions.push(section);
                    existing.permissions = this.formatPermissions(existing.actions);
                } else {
                    rowMap.set(key, {
                        identity: group.identity,
                        type: 'group',
                        permissions: section,
                        identifier: group.identifier,
                        actions: [section]
                    });
                }
            });
        });

        return Array.from(rowMap.values());
    }

    private formatPermissions(actions: PolicySection[]): string {
        return actions.sort().join(', ');
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortPolicies(this.dataSource.data, sort);
    }

    sortPolicies(data: PolicyTableRow[], sort: Sort): PolicyTableRow[] {
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
                case 'permissions':
                    retVal = this.nifiCommon.compareString(a.permissions, b.permissions);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    addPolicy(): void {
        if (!this.latestOptions) {
            return;
        }

        // Get list of existing users and groups across all policy types
        const existingUsers = new Set<string>();
        const existingGroups = new Set<string>();

        Object.values(this.latestSelection).forEach((selection) => {
            if (selection) {
                selection.users.forEach((user) => existingUsers.add(user.identity));
                selection.userGroups.forEach((group) => existingGroups.add(group.identity));
            }
        });

        const dialogRef = this.dialog.open(AddPolicyToBucketDialogComponent, {
            ...LARGE_DIALOG,
            autoFocus: false,
            data: {
                bucket: this.data.bucket,
                existingUsers: Array.from(existingUsers),
                existingGroups: Array.from(existingGroups),
                availableUsers: this.latestOptions.users.map((u) => u.subject),
                availableGroups: this.latestOptions.groups.map((g) => g.subject)
            }
        });

        dialogRef.afterClosed().subscribe((result: AddPolicyResult | undefined) => {
            if (result) {
                // Emit save requests
                result.permissions.forEach((action, index) => {
                    const currentSelection = this.latestSelection[action];

                    // Create copies of arrays (state arrays are immutable)
                    const users = [...(currentSelection?.users || [])];
                    const userGroups = [...(currentSelection?.userGroups || [])];

                    if (result.userOrGroup.type === 'user') {
                        users.push(result.userOrGroup);
                    } else {
                        userGroups.push(result.userOrGroup);
                    }

                    const isLast = index === result.permissions.length - 1;

                    this.savePolicies.emit({
                        bucketId: this.data.bucket.identifier,
                        action,
                        policyId: currentSelection?.policyId,
                        revision: currentSelection?.revision,
                        users,
                        userGroups,
                        isLastInBatch: isLast
                    });
                });
            }
        });
    }

    editPolicy(row: PolicyTableRow): void {
        const dialogRef = this.dialog.open(EditPolicyDialogComponent, {
            ...MEDIUM_DIALOG,
            data: {
                identity: row.identity,
                type: row.type,
                currentPermissions: row.actions
            }
        });

        dialogRef.afterClosed().subscribe((result: EditPolicyResult | undefined) => {
            if (result) {
                const previousPermissions = new Set(row.actions);
                const newPermissions = new Set(result.permissions);

                // Determine which permissions to add and which to remove
                const toAdd: PolicySection[] = result.permissions.filter((p) => !previousPermissions.has(p));
                const toRemove: PolicySection[] = row.actions.filter((p) => !newPermissions.has(p));

                const allActions = [...toAdd, ...toRemove];

                // Process all changes
                allActions.forEach((action, index) => {
                    const currentSelection = this.latestSelection[action];
                    if (!currentSelection) return;

                    // Create copies of arrays
                    let users = [...currentSelection.users];
                    let userGroups = [...currentSelection.userGroups];

                    if (newPermissions.has(action)) {
                        // Add to this policy
                        const subject = {
                            identifier: row.identifier,
                            identity: row.identity,
                            type: row.type
                        };

                        if (row.type === 'user') {
                            // Check if not already present
                            if (!users.find((u) => u.identifier === row.identifier)) {
                                users.push(subject);
                            }
                        } else {
                            // Check if not already present
                            if (!userGroups.find((g) => g.identifier === row.identifier)) {
                                userGroups.push(subject);
                            }
                        }
                    } else {
                        // Remove from this policy
                        users = users.filter((u) => u.identifier !== row.identifier);
                        userGroups = userGroups.filter((g) => g.identifier !== row.identifier);
                    }

                    const isLast = index === allActions.length - 1;

                    this.savePolicies.emit({
                        bucketId: this.data.bucket.identifier,
                        action,
                        policyId: currentSelection.policyId,
                        revision: currentSelection.revision,
                        users,
                        userGroups,
                        isLastInBatch: isLast
                    });
                });
            }
        });
    }

    removePolicy(row: PolicyTableRow): void {
        const dialogRef = this.dialog.open(YesNoDialog, {
            ...SMALL_DIALOG,
            data: {
                title: 'Delete Policy',
                message: `All permissions granted by this policy will be removed for ${row.identity}.`
            }
        });

        dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
            // Remove this user/group from each policy they have
            row.actions.forEach((action, index) => {
                const currentSelection = this.latestSelection[action];
                if (!currentSelection) return;

                // Create copies and filter out the removed user/group
                const users = [...currentSelection.users].filter((u) => u.identifier !== row.identifier);
                const userGroups = [...currentSelection.userGroups].filter((g) => g.identifier !== row.identifier);

                const isLast = index === row.actions.length - 1;

                this.savePolicies.emit({
                    bucketId: this.data.bucket.identifier,
                    action,
                    policyId: currentSelection.policyId,
                    revision: currentSelection.revision,
                    users,
                    userGroups,
                    isLastInBatch: isLast
                });
            });
        });
    }

    close(): void {
        this.dialogRef.close();
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
