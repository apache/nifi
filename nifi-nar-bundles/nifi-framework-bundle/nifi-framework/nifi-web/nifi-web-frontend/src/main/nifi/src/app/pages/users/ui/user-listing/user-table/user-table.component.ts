/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { AfterViewInit, Component, EventEmitter, Input, Output } from '@angular/core';
import { MatTableDataSource } from '@angular/material/table';
import { Sort } from '@angular/material/sort';
import { FormBuilder, FormGroup } from '@angular/forms';
import { debounceTime } from 'rxjs';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { UserEntity, UserGroupEntity } from '../../../state/user-listing';

export interface TenantItem {
    id: string;
    user: string;
    tenantType: 'user' | 'userGroup';
    membership: string[];
}

export interface Tenants {
    users: UserEntity[];
    userGroups: UserGroupEntity[];
}

@Component({
    selector: 'user-table',
    templateUrl: './user-table.component.html',
    styleUrls: ['./user-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class UserTable implements AfterViewInit {
    filterTerm: string = '';
    filterColumn: 'user' | 'membership' = 'user';
    totalCount: number = 0;
    filteredCount: number = 0;

    displayedColumns: string[] = ['user', 'membership', 'actions'];
    dataSource: MatTableDataSource<TenantItem> = new MatTableDataSource<TenantItem>();
    filterForm: FormGroup;

    userLookup: Map<string, UserEntity> = new Map<string, UserEntity>();
    userGroupLookup: Map<string, UserGroupEntity> = new Map<string, UserGroupEntity>();

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
                membership: user.component.userGroups.map((userGroup) => userGroup.component.identity)
            });
        });
        tenants.userGroups.forEach((userGroup) => {
            this.userGroupLookup.set(userGroup.id, userGroup);
            tenantItems.push({
                id: userGroup.id,
                tenantType: 'userGroup',
                user: userGroup.component.identity,
                membership: userGroup.component.users.map((user) => user.component.identity)
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

    @Output() editUser: EventEmitter<UserEntity> = new EventEmitter<UserEntity>();
    @Output() editUserGroup: EventEmitter<UserGroupEntity> = new EventEmitter<UserGroupEntity>();
    @Output() deleteUser: EventEmitter<UserEntity> = new EventEmitter<UserEntity>();
    @Output() deleteUserGroup: EventEmitter<UserGroupEntity> = new EventEmitter<UserGroupEntity>();

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
            ?.valueChanges.pipe(debounceTime(500))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm.get('filterColumn')?.valueChanges.subscribe((filterColumn: string) => {
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
        return item.membership.join(', ');
    }

    sortUsers(items: TenantItem[], sort: Sort): TenantItem[] {
        const data: TenantItem[] = items.slice();
        return data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';

            let retVal: number = 0;
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
}
