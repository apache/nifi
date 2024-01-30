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

import { Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SummaryTableFilterModule } from '../../common/summary-table-filter/summary-table-filter.module';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MultiSort } from '../../common';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import {
    SummaryTableFilterArgs,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { ConnectionStatusSnapshot, ConnectionStatusSnapshotEntity } from '../../../state/summary-listing';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { ComponentType } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';

export type SupportedColumns = 'name' | 'queue' | 'in' | 'out' | 'threshold' | 'sourceName' | 'destinationName';

@Component({
    selector: 'connection-status-table',
    standalone: true,
    imports: [CommonModule, SummaryTableFilterModule, MatSortModule, RouterLink, MatTableModule, MatPaginatorModule],
    templateUrl: './connection-status-table.component.html',
    styleUrls: ['./connection-status-table.component.scss']
})
export class ConnectionStatusTable {
    private _initialSortColumn: SupportedColumns = 'sourceName';
    private _initialSortDirection: SortDirection = 'asc';

    filterableColumns: SummaryTableFilterColumn[] = [
        { key: 'sourceName', label: 'source' },
        { key: 'name', label: 'name' },
        { key: 'destinationName', label: 'destination' }
    ];

    totalCount = 0;
    filteredCount = 0;

    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    displayedColumns: string[] = [
        'moreDetails',
        'name',
        'queue',
        'threshold',
        'in',
        'sourceName',
        'out',
        'destinationName',
        'actions'
    ];

    dataSource: MatTableDataSource<ConnectionStatusSnapshotEntity> =
        new MatTableDataSource<ConnectionStatusSnapshotEntity>();

    @ViewChild(MatPaginator) paginator!: MatPaginator;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;
    }

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

    @Input() selectedConnectionId!: string;

    @Input() set connections(connections: ConnectionStatusSnapshotEntity[]) {
        if (connections) {
            this.dataSource.data = this.sortEntities(connections, this.multiSort);
            this.dataSource.filterPredicate = (data: ConnectionStatusSnapshotEntity, filter: string) => {
                const { filterTerm, filterColumn } = JSON.parse(filter);

                if (filterTerm === '') {
                    return true;
                }

                const field: string = data.connectionStatusSnapshot[
                    filterColumn as keyof ConnectionStatusSnapshot
                ] as string;
                return this.nifiCommon.stringContains(field, filterTerm, true);
            };

            this.totalCount = connections.length;
            this.filteredCount = connections.length;
        }
    }

    @Input() summaryListingStatus: string | null = null;
    @Input() loadedTimestamp: string | null = null;

    @Output() refresh: EventEmitter<void> = new EventEmitter<void>();
    @Output() viewStatusHistory: EventEmitter<ConnectionStatusSnapshotEntity> =
        new EventEmitter<ConnectionStatusSnapshotEntity>();
    @Output() selectConnection: EventEmitter<ConnectionStatusSnapshotEntity> =
        new EventEmitter<ConnectionStatusSnapshotEntity>();
    @Output() clearSelection: EventEmitter<void> = new EventEmitter<void>();

    resetPaginator(): void {
        if (this.dataSource.paginator) {
            this.dataSource.paginator.firstPage();
        }
    }

    applyFilter(filter: SummaryTableFilterArgs) {
        this.dataSource.filter = JSON.stringify(filter);
        this.filteredCount = this.dataSource.filteredData.length;
        this.resetPaginator();
        this.selectNone();
    }

    paginationChanged(): void {
        // clear out any selection
        this.selectNone();
    }

    private selectNone() {
        this.clearSelection.next();
    }

    getConnectionLink(connection: ConnectionStatusSnapshotEntity): string[] {
        return [
            '/process-groups',
            connection.connectionStatusSnapshot.groupId,
            ComponentType.Connection,
            connection.id
        ];
    }

    select(connection: ConnectionStatusSnapshotEntity): void {
        this.selectConnection.next(connection);
    }

    isSelected(connection: ConnectionStatusSnapshotEntity): boolean {
        if (this.selectedConnectionId) {
            return connection.id === this.selectedConnectionId;
        }
        return false;
    }

    canRead(connection: ConnectionStatusSnapshotEntity): boolean {
        return connection.canRead;
    }

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    formatName(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.name;
    }
    formatSource(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.sourceName;
    }
    formatDestination(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.destinationName;
    }
    formatIn(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.input;
    }
    formatOut(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.output;
    }
    formatQueue(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.queued;
    }
    formatThreshold(connection: ConnectionStatusSnapshotEntity): string {
        return `${connection.connectionStatusSnapshot.percentUseCount}% | ${connection.connectionStatusSnapshot.percentUseBytes}%`;
    }

    viewStatusHistoryClicked(event: MouseEvent, connection: ConnectionStatusSnapshotEntity): void {
        event.stopPropagation();
        this.viewStatusHistory.next(connection);
    }

    private supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
            case 'threshold':
            case 'queue':
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

    private sortEntities(data: ConnectionStatusSnapshotEntity[], sort: Sort): ConnectionStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(
                        a.connectionStatusSnapshot.name,
                        b.connectionStatusSnapshot.name
                    );
                    break;
                case 'sourceName':
                    retVal = this.nifiCommon.compareString(
                        a.connectionStatusSnapshot.sourceName,
                        b.connectionStatusSnapshot.sourceName
                    );
                    break;
                case 'destinationName':
                    retVal = this.nifiCommon.compareString(
                        a.connectionStatusSnapshot.destinationName,
                        b.connectionStatusSnapshot.destinationName
                    );
                    break;
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.flowFilesIn,
                            b.connectionStatusSnapshot.flowFilesIn
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.bytesIn,
                            b.connectionStatusSnapshot.bytesIn
                        );
                    }
                    break;
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.flowFilesOut,
                            b.connectionStatusSnapshot.flowFilesOut
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.bytesOut,
                            b.connectionStatusSnapshot.bytesOut
                        );
                    }
                    break;
                case 'queue':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.flowFilesQueued,
                            b.connectionStatusSnapshot.flowFilesQueued
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.bytesQueued,
                            b.connectionStatusSnapshot.bytesQueued
                        );
                    }
                    break;
                case 'threshold':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.percentUseCount,
                            b.connectionStatusSnapshot.percentUseCount
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.percentUseBytes,
                            b.connectionStatusSnapshot.percentUseBytes
                        );
                    }
                    break;
                default:
                    retVal = 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
