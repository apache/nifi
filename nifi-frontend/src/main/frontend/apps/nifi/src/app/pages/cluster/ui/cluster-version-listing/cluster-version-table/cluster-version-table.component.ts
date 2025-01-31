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

import { Component } from '@angular/core';
import {
    ClusterTableFilter,
    ClusterTableFilterColumn
} from '../../common/cluster-table-filter/cluster-table-filter.component';
import { MatTableModule } from '@angular/material/table';
import { NiFiCommon } from '@nifi/shared';
import { NodeSnapshot } from '../../../../../state/system-diagnostics';
import { ClusterTable } from '../../common/cluster-table/cluster-table.component';
import { MatSortModule, Sort } from '@angular/material/sort';

@Component({
    selector: 'cluster-version-table',
    imports: [ClusterTableFilter, MatTableModule, MatSortModule],
    templateUrl: './cluster-version-table.component.html',
    styleUrl: './cluster-version-table.component.scss'
})
export class ClusterVersionTable extends ClusterTable<NodeSnapshot> {
    filterableColumns: ClusterTableFilterColumn[] = [{ key: 'address', label: 'Address' }];

    displayedColumns: string[] = [
        'address',
        'nifiVersion',
        'javaVendor',
        'javaVersion',
        'osName',
        'osVersion',
        'osArchitecture'
    ];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    formatNodeAddress(item: NodeSnapshot): string {
        return `${item.address}:${item.apiPort}`;
    }

    override filterPredicate(item: NodeSnapshot, filter: string): boolean {
        const { filterTerm, filterColumn } = JSON.parse(filter);
        if (filterTerm === '') {
            return true;
        }

        let field = '';
        switch (filterColumn) {
            case 'address':
                field = this.formatNodeAddress(item);
                break;
        }
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    override sortEntities(data: NodeSnapshot[], sort: Sort): NodeSnapshot[] {
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
                case 'nifiVersion':
                    retVal = this.nifiCommon.compareVersion(
                        a.snapshot.versionInfo.niFiVersion,
                        b.snapshot.versionInfo.niFiVersion
                    );
                    break;
                case 'javaVendor':
                    retVal = this.nifiCommon.compareString(
                        a.snapshot.versionInfo.javaVendor,
                        b.snapshot.versionInfo.javaVendor
                    );
                    break;
                case 'javaVersion':
                    retVal = this.nifiCommon.compareVersion(
                        a.snapshot.versionInfo.javaVersion,
                        b.snapshot.versionInfo.javaVersion
                    );
                    break;
                case 'osName':
                    retVal = this.nifiCommon.compareString(
                        a.snapshot.versionInfo.osName,
                        b.snapshot.versionInfo.osName
                    );
                    break;
                case 'osVersion':
                    retVal = this.nifiCommon.compareVersion(
                        a.snapshot.versionInfo.osVersion,
                        b.snapshot.versionInfo.osVersion
                    );
                    break;
                case 'osArchitecture':
                    retVal = this.nifiCommon.compareString(
                        a.snapshot.versionInfo.osArchitecture,
                        b.snapshot.versionInfo.osArchitecture
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

    select(item: NodeSnapshot): any {
        this.selectComponent.next(item);
    }

    isSelected(item: NodeSnapshot): boolean {
        if (this.selectedId) {
            return this.selectedId === item.nodeId;
        }
        return false;
    }
}
