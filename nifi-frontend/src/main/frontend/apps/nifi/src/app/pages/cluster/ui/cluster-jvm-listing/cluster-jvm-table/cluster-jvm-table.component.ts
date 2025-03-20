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
import { ClusterTable } from '../../common/cluster-table/cluster-table.component';
import { NodeSnapshot } from '../../../../../state/system-diagnostics';
import {
    ClusterTableFilter,
    ClusterTableFilterColumn
} from '../../common/cluster-table-filter/cluster-table-filter.component';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { NifiTooltipDirective, NiFiCommon } from '@nifi/shared';
import { GarbageCollectionTipInput } from '../../../../../state/shared';
import { GarbageCollectionTip } from '../../../../../ui/common/tooltips/garbage-collection-tip/garbage-collection-tip.component';
import { ConnectedPosition } from '@angular/cdk/overlay';

@Component({
    selector: 'cluster-jvm-table',
    imports: [ClusterTableFilter, MatTableModule, MatSortModule, NifiTooltipDirective],
    templateUrl: './cluster-jvm-table.component.html',
    styleUrl: './cluster-jvm-table.component.scss'
})
export class ClusterJvmTable extends ClusterTable<NodeSnapshot> {
    filterableColumns: ClusterTableFilterColumn[] = [{ key: 'address', label: 'Address' }];

    displayedColumns: string[] = [
        'address',
        'maxHeap',
        'totalHeap',
        'usedHeap',
        'heapUtilization',
        'totalNonHeap',
        'usedNonHeap',
        'garbageCollection',
        'uptime'
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

    override sortEntities(data: NodeSnapshot[], sort: Sort): any[] {
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
                case 'maxHeap':
                    retVal = this.nifiCommon.compareNumber(a.snapshot.maxHeapBytes, b.snapshot.maxHeapBytes);
                    break;
                case 'totalHeap':
                    retVal = this.nifiCommon.compareNumber(a.snapshot.totalHeapBytes, b.snapshot.totalHeapBytes);
                    break;
                case 'usedHeap':
                    retVal = this.nifiCommon.compareNumber(a.snapshot.usedHeapBytes, b.snapshot.usedHeapBytes);
                    break;
                case 'heapUtilization':
                    retVal = this.nifiCommon.compareNumber(
                        a.snapshot.usedHeapBytes / a.snapshot.totalHeapBytes,
                        b.snapshot.usedHeapBytes / b.snapshot.totalHeapBytes
                    );
                    break;
                case 'totalNonHeap':
                    retVal = this.nifiCommon.compareNumber(a.snapshot.totalNonHeapBytes, b.snapshot.totalNonHeapBytes);
                    break;
                case 'usedNonHeap':
                    retVal = this.nifiCommon.compareNumber(a.snapshot.usedNonHeapBytes, b.snapshot.usedNonHeapBytes);
                    break;
                case 'uptime':
                    retVal = this.nifiCommon.compareNumber(
                        this.nifiCommon.parseDuration(a.snapshot.uptime),
                        this.nifiCommon.parseDuration(b.snapshot.uptime)
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

    getGarbageCollectionTipData(item: NodeSnapshot): GarbageCollectionTipInput {
        return {
            garbageCollections: item.snapshot.garbageCollection
        };
    }

    getGcTooltipPosition(): ConnectedPosition {
        return {
            originX: 'end',
            originY: 'bottom',
            overlayX: 'end',
            overlayY: 'top',
            offsetX: -8,
            offsetY: 8
        };
    }

    protected readonly GarbageCollectionTipComponent = GarbageCollectionTip;
}
