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

import { AfterViewInit, Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SummaryTableFilterModule } from '../../common/summary-table-filter/summary-table-filter.module';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import {
    SummaryTableFilterArgs,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { MultiSort } from '../../common';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import {
    RemoteProcessGroupStatusSnapshot,
    RemoteProcessGroupStatusSnapshotEntity
} from '../../../state/summary-listing';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { ComponentType } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';

export type SupportedColumns = 'name' | 'uri' | 'transmitting' | 'sent' | 'received';

@Component({
    selector: 'remote-process-group-status-table',
    standalone: true,
    imports: [CommonModule, SummaryTableFilterModule, MatSortModule, MatTableModule, RouterLink, MatPaginatorModule],
    templateUrl: './remote-process-group-status-table.component.html',
    styleUrls: ['./remote-process-group-status-table.component.scss']
})
export class RemoteProcessGroupStatusTable implements AfterViewInit {
    private _initialSortColumn: SupportedColumns = 'name';
    private _initialSortDirection: SortDirection = 'asc';

    filterableColumns: SummaryTableFilterColumn[] = [
        { key: 'name', label: 'name' },
        { key: 'targetUri', label: 'uri' }
    ];

    totalCount = 0;
    filteredCount = 0;

    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    displayedColumns: string[] = ['moreDetails', 'name', 'uri', 'transmitting', 'sent', 'received', 'actions'];

    dataSource: MatTableDataSource<RemoteProcessGroupStatusSnapshotEntity> =
        new MatTableDataSource<RemoteProcessGroupStatusSnapshotEntity>();

    @ViewChild(MatPaginator) paginator!: MatPaginator;

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;
    }

    constructor(private nifiCommon: NiFiCommon) {}

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

    @Input() selectedRemoteProcessGroupId!: string;

    @Input() set remoteProcessGroups(rpgs: RemoteProcessGroupStatusSnapshotEntity[]) {
        if (rpgs) {
            this.dataSource.data = this.sortEntities(rpgs, this.multiSort);
            this.dataSource.filterPredicate = (data: RemoteProcessGroupStatusSnapshotEntity, filter: string) => {
                const { filterTerm, filterColumn } = JSON.parse(filter);

                if (filterTerm === '') {
                    return true;
                }

                const field: string = data.remoteProcessGroupStatusSnapshot[
                    filterColumn as keyof RemoteProcessGroupStatusSnapshot
                ] as string;
                return this.nifiCommon.stringContains(field, filterTerm, true);
            };

            this.totalCount = rpgs.length;
            this.filteredCount = rpgs.length;
        }
    }

    @Input() summaryListingStatus: string | null = null;
    @Input() loadedTimestamp: string | null = null;

    @Output() refresh: EventEmitter<void> = new EventEmitter<void>();
    @Output() viewStatusHistory: EventEmitter<RemoteProcessGroupStatusSnapshotEntity> =
        new EventEmitter<RemoteProcessGroupStatusSnapshotEntity>();
    @Output() selectRemoteProcessGroup: EventEmitter<RemoteProcessGroupStatusSnapshotEntity> =
        new EventEmitter<RemoteProcessGroupStatusSnapshotEntity>();
    @Output() clearSelection: EventEmitter<void> = new EventEmitter<void>();

    applyFilter(filter: SummaryTableFilterArgs) {
        this.dataSource.filter = JSON.stringify(filter);
        this.filteredCount = this.dataSource.filteredData.length;
        this.resetPaginator();
        this.selectNone();
    }

    resetPaginator(): void {
        if (this.dataSource.paginator) {
            this.dataSource.paginator.firstPage();
        }
    }

    paginationChanged(): void {
        // clear out any selection
        this.selectNone();
    }

    getRemoteProcessGroupLink(rpg: RemoteProcessGroupStatusSnapshotEntity): string[] {
        return [
            '/process-groups',
            rpg.remoteProcessGroupStatusSnapshot.groupId,
            ComponentType.RemoteProcessGroup,
            rpg.id
        ];
    }

    select(rpg: RemoteProcessGroupStatusSnapshotEntity) {
        this.selectRemoteProcessGroup.next(rpg);
    }

    private selectNone() {
        this.clearSelection.next();
    }

    isSelected(rpg: RemoteProcessGroupStatusSnapshotEntity): boolean {
        if (this.selectedRemoteProcessGroupId) {
            return rpg.id === this.selectedRemoteProcessGroupId;
        }
        return false;
    }

    canRead(rpg: RemoteProcessGroupStatusSnapshotEntity): boolean {
        return rpg.canRead;
    }

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    viewStatusHistoryClicked(event: MouseEvent, rpg: RemoteProcessGroupStatusSnapshotEntity): void {
        event.stopPropagation();
        this.viewStatusHistory.next(rpg);
    }

    formatName(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.name;
    }

    formatTransmitting(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        if (rpg.remoteProcessGroupStatusSnapshot.transmissionStatus === 'Transmitting') {
            return rpg.remoteProcessGroupStatusSnapshot.transmissionStatus;
        } else {
            return 'Not Transmitting';
        }
    }

    formatUri(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.targetUri;
    }

    formatSent(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.sent;
    }

    formatReceived(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.received;
    }

    getTransmissionStatusIcon(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        if (rpg.remoteProcessGroupStatusSnapshot.transmissionStatus === 'Transmitting') {
            return 'transmitting fa fa-bullseye';
        } else {
            return 'not-transmitting icon icon-transmit-false';
        }
    }

    private supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'sent':
            case 'received':
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

    private sortEntities(
        data: RemoteProcessGroupStatusSnapshotEntity[],
        sort: Sort
    ): RemoteProcessGroupStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }

        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(
                        a.remoteProcessGroupStatusSnapshot.name,
                        b.remoteProcessGroupStatusSnapshot.name
                    );
                    break;
                case 'transmitting':
                    retVal = this.nifiCommon.compareString(
                        a.remoteProcessGroupStatusSnapshot.transmissionStatus,
                        b.remoteProcessGroupStatusSnapshot.transmissionStatus
                    );
                    break;
                case 'uri':
                    retVal = this.nifiCommon.compareString(
                        a.remoteProcessGroupStatusSnapshot.targetUri,
                        b.remoteProcessGroupStatusSnapshot.targetUri
                    );
                    break;
                case 'sent':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.flowFilesSent,
                            b.remoteProcessGroupStatusSnapshot.flowFilesSent
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.bytesSent,
                            b.remoteProcessGroupStatusSnapshot.bytesSent
                        );
                    }
                    break;
                case 'received':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.flowFilesReceived,
                            b.remoteProcessGroupStatusSnapshot.flowFilesReceived
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.bytesReceived,
                            b.remoteProcessGroupStatusSnapshot.bytesReceived
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
