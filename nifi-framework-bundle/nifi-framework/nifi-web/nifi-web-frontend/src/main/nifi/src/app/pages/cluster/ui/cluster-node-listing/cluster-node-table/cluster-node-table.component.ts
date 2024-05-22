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
import { MatCell, MatHeaderCell, MatTableModule } from '@angular/material/table';
import { ClusterNode } from '../../../state/cluster-listing';
import { MatSortHeader, MatSortModule, Sort } from '@angular/material/sort';
import { NgClass } from '@angular/common';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { ClusterTable } from '../../common/cluster-table/cluster-table.component';
import {
    ClusterTableFilter,
    ClusterTableFilterColumn
} from '../../common/cluster-table-filter/cluster-table-filter.component';
import { CurrentUser } from '../../../../../state/current-user';
import { MatIconButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@Component({
    selector: 'cluster-node-table',
    standalone: true,
    imports: [
        MatCell,
        MatHeaderCell,
        MatSortHeader,
        NgClass,
        MatTableModule,
        MatSortModule,
        ClusterTableFilter,
        MatIconButton,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    templateUrl: './cluster-node-table.component.html',
    styleUrl: './cluster-node-table.component.scss'
})
export class ClusterNodeTable extends ClusterTable<ClusterNode> {
    private _currentUser!: CurrentUser;

    @Output() disconnectNode: EventEmitter<ClusterNode> = new EventEmitter<ClusterNode>();
    @Output() connectNode: EventEmitter<ClusterNode> = new EventEmitter<ClusterNode>();
    @Output() offloadNode: EventEmitter<ClusterNode> = new EventEmitter<ClusterNode>();
    @Output() removeNode: EventEmitter<ClusterNode> = new EventEmitter<ClusterNode>();
    @Output() showDetail: EventEmitter<ClusterNode> = new EventEmitter<ClusterNode>();

    @Input() set currentUser(user: CurrentUser) {
        this._currentUser = user;
        const actionsColIndex = this.displayedColumns.findIndex((col) => col === 'actions');
        if (user.controllerPermissions.canRead && user.controllerPermissions.canWrite) {
            if (actionsColIndex < 0) {
                this.displayedColumns.push('actions');
            }
        } else {
            if (actionsColIndex >= 0) {
                this.displayedColumns.splice(actionsColIndex, 1);
            }
        }
    }
    get currentUser() {
        return this._currentUser;
    }

    filterableColumns: ClusterTableFilterColumn[] = [
        { key: 'address', label: 'Address' },
        { key: 'status', label: 'Status' }
    ];

    displayedColumns: string[] = [
        'moreDetails',
        'address',
        'activeThreadCount',
        'queued',
        'status',
        'nodeStartTime',
        'heartbeat'
    ];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    formatNodeAddress(item: ClusterNode): string {
        return `${item.address}:${item.apiPort}`;
    }

    formatActiveThreadCount(item: ClusterNode): number | null {
        return item.status === 'CONNECTED' ? item.activeThreadCount || 0 : null;
    }

    formatQueued(item: ClusterNode): string {
        return item.queued || '';
    }

    formatStatus(item: ClusterNode): string {
        let status = item.status;
        if (item.roles.includes('Primary Node')) {
            status += ', PRIMARY';
        }
        if (item.roles.includes('Cluster Coordinator')) {
            status += ', COORDINATOR';
        }
        return status;
    }

    formatStartTime(item: ClusterNode): string {
        return item.nodeStartTime || '';
    }

    formatHeartbeat(item: ClusterNode): string {
        return item.heartbeat || '';
    }

    override filterPredicate(item: ClusterNode, filter: string): boolean {
        const { filterTerm, filterColumn } = JSON.parse(filter);
        if (filterTerm === '') {
            return true;
        }

        let field = '';
        switch (filterColumn) {
            case 'address':
                field = this.formatNodeAddress(item);
                break;
            case 'status':
                field = this.formatStatus(item);
                break;
        }
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    override sortEntities(data: ClusterNode[], sort: Sort): ClusterNode[] {
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
                case 'activeThreadCount':
                    retVal = this.nifiCommon.compareNumber(
                        this.formatActiveThreadCount(a),
                        this.formatActiveThreadCount(b)
                    );
                    break;
                case 'status':
                    retVal = this.nifiCommon.compareString(this.formatStatus(a), this.formatStatus(b));
                    break;
                case 'nodeStartTime':
                    retVal = this.nifiCommon.compareNumber(
                        this.nifiCommon.parseDateTime(a.nodeStartTime).getTime(),
                        this.nifiCommon.parseDateTime(b.nodeStartTime).getTime()
                    );
                    break;
                case 'heartbeat':
                    retVal = this.nifiCommon.compareNumber(
                        this.nifiCommon.parseDateTime(a.heartbeat).getTime(),
                        this.nifiCommon.parseDateTime(b.heartbeat).getTime()
                    );
                    break;
                case 'queued':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(a.flowFilesQueued, b.flowFilesQueued);
                    } else {
                        retVal = this.nifiCommon.compareNumber(a.bytesQueued, b.bytesQueued);
                    }
                    break;
                default:
                    retVal = 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'queued':
                return true;
            default:
                return false;
        }
    }

    disconnect(item: ClusterNode) {
        this.disconnectNode.next(item);
    }

    connect(item: ClusterNode) {
        this.connectNode.next(item);
    }

    offload(item: ClusterNode) {
        this.offloadNode.next(item);
    }

    remove(item: ClusterNode) {
        this.removeNode.next(item);
    }

    moreDetail(item: ClusterNode) {
        this.showDetail.next(item);
    }

    select(item: ClusterNode) {
        this.selectComponent.next(item);
    }

    isSelected(item: ClusterNode): boolean {
        if (this.selectedId) {
            return this.selectedId === item.nodeId;
        }
        return false;
    }
}
