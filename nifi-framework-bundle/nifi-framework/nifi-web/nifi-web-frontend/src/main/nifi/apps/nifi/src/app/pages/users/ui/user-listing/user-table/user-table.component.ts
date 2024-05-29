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

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, Input, Output } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { debounceTime } from 'rxjs';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { CurrentUser } from '../../../../../state/current-user';
import { AccessPolicySummaryEntity, UserEntity, UserGroupEntity } from '../../../../../state/shared';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';

import { MatInputModule } from '@angular/material/input';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export interface TenantItem {
    id: string;
    user: string;
    tenantType: 'user' | 'userGroup';
    membership: string[];
    configurable: boolean;
}

export interface Tenants {
    users: UserEntity[];
    userGroups: UserGroupEntity[];
}

@Component({
    selector: 'user-table',
    standalone: true,
    templateUrl: './user-table.component.html',
    imports: [
        ReactiveFormsModule,
        MatFormFieldModule,
        MatSelectModule,
        MatTableModule,
        MatSortModule,
        MatInputModule,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    styleUrls: ['./user-table.component.scss']
})
export class UserTable implements AfterViewInit {
    filterTerm = '';
    filterColumn: 'user' | 'membership' = 'user';
    totalCount = 0;
    filteredCount = 0;

    displayedColumns: string[] = ['user', 'membership', 'actions'];
    dataSource: MatTableDataSource<TenantItem> = new MatTableDataSource<TenantItem>();
    filterForm: FormGroup;

    userLookup: Map<string, UserEntity> = new Map<string, UserEntity>();
    userGroupLookup: Map<string, UserGroupEntity> = new Map<string, UserGroupEntity>();
    private destroyRef: DestroyRef = inject(DestroyRef);

    @Input() set tenants(tenants: Tenants) {
        this.userLookup.clear();
        this.userGroupLookup.clear();

        const tenantItems: TenantItem[] = [];
        tenants.users.forEach((user) => {
            this.userLookup.set(user.id, user);
            tenantItems.push({
                id: user.id,
                tenantType: 'user',
                user: user.component.identity,
                membership: user.component.userGroups.map((userGroup) => userGroup.component.identity),
                configurable: user.component.configurable
            });
        });
        tenants.userGroups.forEach((userGroup) => {
            this.userGroupLookup.set(userGroup.id, userGroup);
            tenantItems.push({
                id: userGroup.id,
                tenantType: 'userGroup',
                user: userGroup.component.identity,
                membership: userGroup.component.users.map((user) => user.component.identity),
                configurable: userGroup.component.configurable
            });
        });

        this.dataSource.data = this.sortUsers(tenantItems, this.sort);
        this.dataSource.filterPredicate = (data: TenantItem, filter: string) => {
            const { filterTerm, filterColumn } = JSON.parse(filter);
            if (filterColumn === 'user') {
                return this.nifiCommon.stringContains(data.user, filterTerm, true);
            } else {
                return this.nifiCommon.stringContains(this.formatMembership(data), filterTerm, true);
            }
        };
        this.totalCount = tenantItems.length;
        this.filteredCount = tenantItems.length;

        // apply any filtering to the new data
        const filterTerm = this.filterForm.get('filterTerm')?.value;
        if (filterTerm?.length > 0) {
            const filterColumn = this.filterForm.get('filterColumn')?.value;
            this.applyFilter(filterTerm, filterColumn);
        }
    }

    @Input() selectedTenantId!: string;
    @Input() currentUser!: CurrentUser;
    @Input() configurableUsersAndGroups!: boolean;

    @Output() createTenant: EventEmitter<void> = new EventEmitter<void>();
    @Output() selectTenant: EventEmitter<string> = new EventEmitter<string>();
    @Output() editTenant: EventEmitter<string> = new EventEmitter<string>();
    @Output() deleteUser: EventEmitter<UserEntity> = new EventEmitter<UserEntity>();
    @Output() deleteUserGroup: EventEmitter<UserGroupEntity> = new EventEmitter<UserGroupEntity>();
    @Output() viewAccessPolicies: EventEmitter<string> = new EventEmitter<string>();

    sort: Sort = {
        active: 'user',
        direction: 'asc'
    };

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: 'user' });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });
    }

    applyFilter(filterTerm: string, filterColumn: string) {
        this.dataSource.filter = JSON.stringify({ filterTerm, filterColumn });
        this.filteredCount = this.dataSource.filteredData.length;
    }

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortUsers(this.dataSource.data, sort);
    }

    formatMembership(item: TenantItem): string {
        return item.membership.sort((a, b) => this.nifiCommon.compareString(a, b)).join(', ');
    }

    sortUsers(items: TenantItem[], sort: Sort): TenantItem[] {
        const data: TenantItem[] = items.slice();
        return data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';

            let retVal = 0;
            switch (sort.active) {
                case 'user':
                    retVal = this.nifiCommon.compareString(a.user, b.user);
                    break;
                case 'membership':
                    retVal = this.nifiCommon.compareString(this.formatMembership(a), this.formatMembership(b));
                    break;
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    select(item: TenantItem): void {
        this.selectTenant.next(item.id);
    }

    isSelected(item: TenantItem): boolean {
        if (this.selectedTenantId) {
            return item.id == this.selectedTenantId;
        }
        return false;
    }

    canModifyTenants(currentUser: CurrentUser): boolean {
        return (
            currentUser.tenantsPermissions.canRead &&
            currentUser.tenantsPermissions.canWrite &&
            this.configurableUsersAndGroups
        );
    }

    createClicked(): void {
        this.createTenant.next();
    }

    canEditOrDelete(currentUser: CurrentUser, item: TenantItem): boolean {
        return this.canModifyTenants(currentUser) && item.configurable;
    }

    editClicked(item: TenantItem): void {
        this.editTenant.next(item.id);
    }

    deleteClicked(item: TenantItem): void {
        if (item.tenantType === 'user') {
            const user: UserEntity | undefined = this.userLookup.get(item.id);
            if (user) {
                this.deleteUser.next(user);
            }
        } else if (item.tenantType === 'userGroup') {
            const userGroup: UserGroupEntity | undefined = this.userGroupLookup.get(item.id);
            if (userGroup) {
                this.deleteUserGroup.next(userGroup);
            }
        }
    }

    private getAccessPolicies(item: TenantItem): AccessPolicySummaryEntity[] {
        const accessPolicies: AccessPolicySummaryEntity[] = [];
        if (item.tenantType === 'user') {
            const user: UserEntity | undefined = this.userLookup.get(item.id);
            if (user) {
                accessPolicies.push(...user.component.accessPolicies);
            }
        } else if (item.tenantType === 'userGroup') {
            const userGroup: UserGroupEntity | undefined = this.userGroupLookup.get(item.id);
            if (userGroup) {
                accessPolicies.push(...userGroup.component.accessPolicies);
            }
        }
        return accessPolicies;
    }

    hasAccessPolicies(item: TenantItem): boolean {
        return !this.nifiCommon.isEmpty(this.getAccessPolicies(item));
    }

    viewAccessPoliciesClicked(item: TenantItem): void {
        this.viewAccessPolicies.next(item.id);
    }
}
