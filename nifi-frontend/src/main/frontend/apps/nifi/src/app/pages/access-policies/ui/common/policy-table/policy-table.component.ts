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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NiFiCommon } from '@nifi/shared';
import { TenantEntity } from '../../../../../state/shared';

import { AccessPolicyEntity } from '../../../state/shared';
import { RemoveTenantFromPolicyRequest } from '../../../state/access-policy';
import { MatIconButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export interface TenantItem {
    id: string;
    user: string;
    tenantType: 'user' | 'userGroup';
    configurable: boolean;
}

@Component({
    selector: 'policy-table',
    templateUrl: './policy-table.component.html',
    imports: [MatTableModule, MatSortModule, MatIconButton, MatMenu, MatMenuTrigger, MatMenuItem],
    styleUrls: ['./policy-table.component.scss']
})
export class PolicyTable {
    displayedColumns: string[] = ['user', 'actions'];
    dataSource: MatTableDataSource<TenantItem> = new MatTableDataSource<TenantItem>();

    tenantLookup: Map<string, TenantEntity> = new Map<string, TenantEntity>();

    @Input() set policy(policy: AccessPolicyEntity | undefined) {
        const tenantItems: TenantItem[] = [];

        if (policy) {
            policy.component.users.forEach((user) => {
                this.tenantLookup.set(user.id, user);
                tenantItems.push({
                    id: user.id,
                    tenantType: 'user',
                    user: user.component.identity,
                    configurable: user.component.configurable
                });
            });
            policy.component.userGroups.forEach((userGroup) => {
                this.tenantLookup.set(userGroup.id, userGroup);
                tenantItems.push({
                    id: userGroup.id,
                    tenantType: 'userGroup',
                    user: userGroup.component.identity,
                    configurable: userGroup.component.configurable
                });
            });
        }

        this.dataSource.data = this.sortUsers(tenantItems, this.sort);
        this._policy = policy;
    }

    @Input() supportsPolicyModification!: boolean;

    @Output() removeTenantFromPolicy: EventEmitter<RemoveTenantFromPolicyRequest> =
        new EventEmitter<RemoveTenantFromPolicyRequest>();

    private _policy: AccessPolicyEntity | undefined;
    selectedTenantId: string | null = null;

    sort: Sort = {
        active: 'user',
        direction: 'asc'
    };

    constructor(private nifiCommon: NiFiCommon) {}

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortUsers(this.dataSource.data, sort);
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
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    select(item: TenantItem): void {
        this.selectedTenantId = item.id;
    }

    isSelected(item: TenantItem): boolean {
        if (this.selectedTenantId) {
            return item.id == this.selectedTenantId;
        }
        return false;
    }

    canRemove(): boolean {
        if (this._policy) {
            return (
                this.supportsPolicyModification &&
                this._policy.permissions.canWrite &&
                this._policy.component.configurable
            );
        }
        return false;
    }

    removeClicked(item: TenantItem): void {
        const tenant: TenantEntity | undefined = this.tenantLookup.get(item.id);
        if (tenant) {
            this.removeTenantFromPolicy.next({
                tenantType: item.tenantType,
                tenant
            });
        }
    }
}
