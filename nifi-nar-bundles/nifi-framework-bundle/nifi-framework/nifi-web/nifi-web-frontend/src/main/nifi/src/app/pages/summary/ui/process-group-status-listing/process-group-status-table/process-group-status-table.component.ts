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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MultiSort } from '../../common';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { SummaryTableFilterModule } from '../../common/summary-table-filter/summary-table-filter.module';
import {
    ProcessGroupStatusSnapshot,
    ProcessGroupStatusSnapshotEntity,
    ProcessorStatusSnapshot,
    ProcessorStatusSnapshotEntity,
    VersionedFlowState
} from '../../../state/summary-listing';
import {
    SummaryTableFilterArgs,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { RouterLink } from '@angular/router';

export type SupportedColumns =
    | 'name'
    | 'versionedFlowState'
    | 'transferred'
    | 'in'
    | 'readWrite'
    | 'out'
    | 'sent'
    | 'received'
    | 'activeThreads'
    | 'tasks';

@Component({
    selector: 'process-group-status-table',
    standalone: true,
    imports: [CommonModule, MatSortModule, MatTableModule, SummaryTableFilterModule, RouterLink],
    templateUrl: './process-group-status-table.component.html',
    styleUrls: ['./process-group-status-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class ProcessGroupStatusTable {
    private _initialSortColumn: SupportedColumns = 'name';
    private _initialSortDirection: SortDirection = 'asc';

    filterableColumns: SummaryTableFilterColumn[] = [{ key: 'name', label: 'name' }];
    totalCount: number = 0;
    filteredCount: number = 0;

    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    displayedColumns: string[] = [
        'moreDetails',
        'name',
        'versionedFlowState',
        'transferred',
        'in',
        'readWrite',
        'out',
        'sent',
        'received',
        'activeThreads',
        'tasks',
        'actions'
    ];

    dataSource: MatTableDataSource<ProcessGroupStatusSnapshotEntity> =
        new MatTableDataSource<ProcessGroupStatusSnapshotEntity>();

    constructor(private nifiCommon: NiFiCommon) {}

    applyFilter(filter: SummaryTableFilterArgs) {
        this.dataSource.filter = JSON.stringify(filter);
        this.filteredCount = this.dataSource.filteredData.length;
    }

    @Input() selectedProcessGroupId!: string;

    @Input() set initialSortColumn(initialSortColumn: SupportedColumns) {
        this._initialSortColumn = initialSortColumn;
        this.multiSort = { ...this.multiSort, active: initialSortColumn };
    }

    get initialSortColumn() {
        return this._initialSortColumn;
    }

    @Input() set initialSortDirection(initialSortDirection: SortDirection) {
        this._initialSortDirection = initialSortDirection;
        this.multiSort = { ...this.multiSort, direction: initialSortDirection };
    }

    get initialSortDirection() {
        return this._initialSortDirection;
    }

    @Input() rootProcessGroup!: ProcessGroupStatusSnapshot;

    @Input() set processGroups(processGroups: ProcessGroupStatusSnapshotEntity[]) {
        if (processGroups) {
            this.dataSource.data = this.sortEntities(processGroups, this.multiSort);

            this.dataSource.filterPredicate = (data: ProcessGroupStatusSnapshotEntity, filter: string): boolean => {
                const { filterTerm, filterColumn } = JSON.parse(filter);

                if (filterTerm === '') {
                    return true;
                }

                try {
                    const field: string = data.processGroupStatusSnapshot[
                        filterColumn as keyof ProcessGroupStatusSnapshot
                    ] as string;
                    return this.nifiCommon.stringContains(field, filterTerm, true);
                } catch (e) {
                    // invalid regex;
                    return false;
                }
            };

            this.totalCount = processGroups.length;
            this.filteredCount = processGroups.length;
        }
    }

    @Output() viewStatusHistory: EventEmitter<ProcessGroupStatusSnapshotEntity> =
        new EventEmitter<ProcessGroupStatusSnapshotEntity>();
    @Output() selectProcessGroup: EventEmitter<ProcessGroupStatusSnapshotEntity> =
        new EventEmitter<ProcessGroupStatusSnapshotEntity>();

    formatName(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.name;
    }

    private versionedFlowStateMap: { [key: string]: { classes: string; label: string } } = {
        STALE: {
            classes: 'fa fa-arrow-circle-up stale',
            label: 'Stale'
        },
        LOCALLY_MODIFIED: {
            classes: 'fa fa-asterisk locally-modified',
            label: 'Locally modified'
        },
        UP_TO_DATE: {
            classes: 'fa fa-check up-to-date',
            label: 'Up to date'
        },
        LOCALLY_MODIFIED_AND_STALE: {
            classes: 'fa fa-exclamation-circle locally-modified-and-stale',
            label: 'Locally modified and stale'
        },
        SYNC_FAILURE: {
            classes: 'fa fa-question sync-failure',
            label: 'Sync failure'
        }
    };
    formatVersionedFlowState(pg: ProcessGroupStatusSnapshotEntity): string {
        if (!pg.processGroupStatusSnapshot.versionedFlowState) {
            return '';
        }
        return this.versionedFlowStateMap[pg.processGroupStatusSnapshot.versionedFlowState].label;
    }

    getVersionedFlowStateIcon(pg: ProcessGroupStatusSnapshotEntity): string {
        if (!pg.processGroupStatusSnapshot.versionedFlowState) {
            return '';
        }
        return this.versionedFlowStateMap[pg.processGroupStatusSnapshot.versionedFlowState].classes;
    }

    formatTransferred(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.transferred;
    }

    formatIn(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.input;
    }

    formatReadWrite(pg: ProcessGroupStatusSnapshotEntity): string {
        return `${pg.processGroupStatusSnapshot.read} | ${pg.processGroupStatusSnapshot.written}`;
    }

    formatOut(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.output;
    }

    formatSent(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.sent;
    }

    formatReceived(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.received;
    }

    formatActiveThreads(pg: ProcessGroupStatusSnapshotEntity): string {
        const percentage: number = this.calculatePercent(
            pg.processGroupStatusSnapshot.activeThreadCount,
            this.rootProcessGroup.activeThreadCount
        );

        return `${pg.processGroupStatusSnapshot.activeThreadCount} (${percentage}%)`;
    }

    formatTasks(pg: ProcessGroupStatusSnapshotEntity): string {
        const percentage: number = this.calculatePercent(
            pg.processGroupStatusSnapshot.processingNanos,
            this.rootProcessGroup.processingNanos
        );

        return `${this.nifiCommon.formatDuration(pg.processGroupStatusSnapshot.processingNanos)} (${percentage}%)`;
    }

    private calculatePercent(used: number, total: number): number {
        if (total !== undefined && total > 0) {
            return Math.round((used / total) * 100);
        }
        return 0;
    }

    private supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'transferred':
            case 'in':
            case 'out':
            case 'readWrite':
            case 'received':
            case 'sent':
            case 'activeThreads':
            case 'tasks':
                return true;
            default:
                return false;
        }
    }

    private setMultiSort(sort: Sort) {
        const { active, direction, sortValueIndex, totalValues } = this.multiSort;

        if (this.supportsMultiValuedSort(sort)) {
            if (active === sort.active) {
                // previous sort was of the same column
                if (direction === 'desc' && sort.direction === 'asc') {
                    // change from previous index to the next
                    const newIndex = sortValueIndex + 1 >= totalValues ? 0 : sortValueIndex + 1;
                    this.multiSort = { ...sort, sortValueIndex: newIndex, totalValues };
                } else {
                    this.multiSort = { ...sort, sortValueIndex, totalValues };
                }
            } else {
                // sorting a different column, just reset
                this.multiSort = { ...sort, sortValueIndex: 0, totalValues };
            }
        } else {
            this.multiSort = { ...sort, sortValueIndex: 0, totalValues };
        }
    }

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ProcessGroupStatusSnapshotEntity[], sort: Sort): ProcessGroupStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }

        let aggregateDuration: number = 0;
        let aggregateActiveThreads: number = 0;
        if (this.rootProcessGroup) {
            aggregateDuration = this.rootProcessGroup.processingNanos;
            aggregateActiveThreads = this.rootProcessGroup.activeThreadCount;
        }

        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal: number = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'versionedFlowState':
                    retVal = this.nifiCommon.compareString(
                        this.formatVersionedFlowState(a),
                        this.formatVersionedFlowState(b)
                    );
                    break;
                case 'transferred':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesTransferred,
                            b.processGroupStatusSnapshot.flowFilesTransferred
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesTransferred,
                            b.processGroupStatusSnapshot.bytesTransferred
                        );
                    }
                    break;
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesIn,
                            b.processGroupStatusSnapshot.flowFilesIn
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesIn,
                            b.processGroupStatusSnapshot.bytesIn
                        );
                    }
                    break;
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesOut,
                            b.processGroupStatusSnapshot.flowFilesOut
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesOut,
                            b.processGroupStatusSnapshot.bytesOut
                        );
                    }
                    break;
                case 'readWrite':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesRead,
                            b.processGroupStatusSnapshot.bytesRead
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesWritten,
                            b.processGroupStatusSnapshot.bytesWritten
                        );
                    }
                    break;
                case 'sent':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesSent,
                            b.processGroupStatusSnapshot.flowFilesSent
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesSent,
                            b.processGroupStatusSnapshot.bytesSent
                        );
                    }
                    break;
                case 'received':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesReceived,
                            b.processGroupStatusSnapshot.flowFilesReceived
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesReceived,
                            b.processGroupStatusSnapshot.bytesReceived
                        );
                    }
                    break;
                case 'activeThreads':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.activeThreadCount,
                            b.processGroupStatusSnapshot.activeThreadCount
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.activeThreadCount / aggregateActiveThreads,
                            b.processGroupStatusSnapshot.activeThreadCount / aggregateActiveThreads
                        );
                    }
                    break;
                case 'tasks':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.processingNanos,
                            b.processGroupStatusSnapshot.processingNanos
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.processingNanos / aggregateDuration,
                            b.processGroupStatusSnapshot.processingNanos / aggregateDuration
                        );
                    }
                    break;
                default:
                    retVal = 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    canRead(pg: ProcessGroupStatusSnapshotEntity) {
        return pg.canRead;
    }

    getProcessGroupLink(pg: ProcessGroupStatusSnapshotEntity): string[] {
        return ['/process-groups', pg.id];
    }

    select(pg: ProcessGroupStatusSnapshotEntity): void {
        this.selectProcessGroup.next(pg);
    }
    isSelected(pg: ProcessGroupStatusSnapshotEntity): boolean {
        if (this.selectedProcessGroupId) {
            return pg.id === this.selectedProcessGroupId;
        }
        return false;
    }

    viewStatusHistoryClicked(event: MouseEvent, pg: ProcessGroupStatusSnapshotEntity): void {
        event.stopPropagation();
        this.viewStatusHistory.next(pg);
    }
}
