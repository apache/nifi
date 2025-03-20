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

import { Component, Input } from '@angular/core';
import { ClusterTable } from '../cluster-table/cluster-table.component';
import { ClusterNodeRepositoryStorageUsage } from '../../../../../state/system-diagnostics';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NiFiCommon } from '@nifi/shared';
import { ClusterTableFilter, ClusterTableFilterColumn } from '../cluster-table-filter/cluster-table-filter.component';
import { MatTableModule } from '@angular/material/table';

@Component({
    selector: 'repository-storage-table',
    imports: [ClusterTableFilter, MatTableModule, MatSortModule],
    templateUrl: './repository-storage-table.component.html',
    styleUrl: './repository-storage-table.component.scss'
})
export class RepositoryStorageTable extends ClusterTable<ClusterNodeRepositoryStorageUsage> {
    filterableColumns: ClusterTableFilterColumn[] = [{ key: 'address', label: 'Address' }];
    displayedColumns: string[] = ['address', 'totalSpace', 'usedSpace', 'freeSpace', 'utilization'];

    @Input() set showRepoIdentifier(value: boolean) {
        if (value) {
            if (this.filterableColumns.length === 1) {
                this.filterableColumns.push({ key: 'repository', label: 'repository' });
                this.displayedColumns.splice(1, 0, 'repository');
            }
        } else {
            if (this.filterableColumns.length > 1) {
                this.filterableColumns.splice(this.filterableColumns.length - 1, 1);
                this.displayedColumns.splice(1, 1);
            }
        }
    }

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    override filterPredicate(item: ClusterNodeRepositoryStorageUsage, filter: string): boolean {
        const { filterTerm, filterColumn } = JSON.parse(filter);
        if (filterTerm === '') {
            return true;
        }

        let field = '';
        switch (filterColumn) {
            case 'address':
                field = this.formatNodeAddress(item);
                break;
            case 'repository':
                field = item.repositoryStorageUsage.identifier || '';
                break;
        }
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    override sortEntities(data: ClusterNodeRepositoryStorageUsage[], sort: Sort): ClusterNodeRepositoryStorageUsage[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'address':
                    retVal = this.nifiCommon.compareString(a.address, b.address);
                    // check the port if the addresses are the same
                    if (retVal === 0) {
                        retVal = this.nifiCommon.compareNumber(a.apiPort, b.apiPort);
                    }
                    break;
                case 'totalSpace':
                    retVal = this.nifiCommon.compareNumber(
                        a.repositoryStorageUsage.totalSpaceBytes,
                        b.repositoryStorageUsage.totalSpaceBytes
                    );
                    break;
                case 'usedSpace':
                    retVal = this.nifiCommon.compareNumber(
                        a.repositoryStorageUsage.usedSpaceBytes,
                        b.repositoryStorageUsage.usedSpaceBytes
                    );
                    break;
                case 'freeSpace':
                    retVal = this.nifiCommon.compareNumber(
                        a.repositoryStorageUsage.freeSpaceBytes,
                        b.repositoryStorageUsage.freeSpaceBytes
                    );
                    break;
                case 'utilization':
                    retVal = this.nifiCommon.compareNumber(
                        a.repositoryStorageUsage.usedSpaceBytes / a.repositoryStorageUsage.totalSpaceBytes,
                        b.repositoryStorageUsage.usedSpaceBytes / b.repositoryStorageUsage.totalSpaceBytes
                    );
                    break;
                case 'repository':
                    retVal = this.nifiCommon.compareString(
                        a.repositoryStorageUsage.identifier,
                        b.repositoryStorageUsage.identifier
                    );
                    break;
                default:
                    retVal = 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    override supportsMultiValuedSort(): boolean {
        return false;
    }

    formatNodeAddress(item: ClusterNodeRepositoryStorageUsage): string {
        return `${item.address}:${item.apiPort}`;
    }

    select(item: ClusterNodeRepositoryStorageUsage): any {
        this.selectComponent.next(item);
    }

    isSelected(item: ClusterNodeRepositoryStorageUsage): boolean {
        if (this.selectedId && this.selectedRepositoryId) {
            return (
                this.selectedId === item.nodeId && this.selectedRepositoryId === item.repositoryStorageUsage.identifier
            );
        }
        return false;
    }
}
