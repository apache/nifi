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
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MultiSort } from '../index';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { PortStatusSnapshot, PortStatusSnapshotEntity } from '../../../state/summary-listing';
import { SummaryTableFilterModule } from '../summary-table-filter/summary-table-filter.module';
import {
    SummaryTableFilterArgs,
    SummaryTableFilterColumn
} from '../summary-table-filter/summary-table-filter.component';
import { ComponentType } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';

export type SupportedColumns = 'name' | 'runStatus' | 'in' | 'out';

@Component({
    selector: 'port-status-table',
    standalone: true,
    imports: [CommonModule, SummaryTableFilterModule, MatSortModule, MatTableModule, RouterLink, MatPaginatorModule],
    templateUrl: './port-status-table.component.html',
    styleUrls: ['./port-status-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class PortStatusTable implements AfterViewInit {
    private _initialSortColumn: SupportedColumns = 'name';
    private _initialSortDirection: SortDirection = 'asc';
    private _portType!: 'input' | 'output';

    filterableColumns: SummaryTableFilterColumn[] = [{ key: 'name', label: 'name' }];

    totalCount = 0;
    filteredCount = 0;

    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    displayedColumns: string[] = [];

    dataSource: MatTableDataSource<PortStatusSnapshotEntity> = new MatTableDataSource<PortStatusSnapshotEntity>();

    @ViewChild(MatPaginator) paginator!: MatPaginator;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;
    }

    @Input() set portType(type: 'input' | 'output') {
        if (type === 'input') {
            this.displayedColumns = ['moreDetails', 'name', 'runStatus', 'in', 'actions'];
        } else {
            this.displayedColumns = ['moreDetails', 'name', 'runStatus', 'out', 'actions'];
        }
        this._portType = type;
    }

    get portType() {
        return this._portType;
    }

    @Input() selectedPortId!: string;

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

    @Input() set ports(ports: PortStatusSnapshotEntity[]) {
        if (ports) {
            this.dataSource.data = this.sortEntities(ports, this.multiSort);
            this.dataSource.filterPredicate = (data: PortStatusSnapshotEntity, filter: string) => {
                const { filterTerm, filterColumn, filterStatus } = JSON.parse(filter);
                const matchOnStatus: boolean = filterStatus !== 'All';

                if (matchOnStatus) {
                    if (data.portStatusSnapshot.runStatus !== filterStatus) {
                        return false;
                    }
                }
                if (filterTerm === '') {
                    return true;
                }

                const field: string = data.portStatusSnapshot[filterColumn as keyof PortStatusSnapshot] as string;
                return this.nifiCommon.stringContains(field, filterTerm, true);
            };

            this.totalCount = ports.length;
            this.filteredCount = ports.length;
        }
    }

    @Input() summaryListingStatus: string | null = null;
    @Input() loadedTimestamp: string | null = null;

    @Output() refresh: EventEmitter<void> = new EventEmitter<void>();
    @Output() selectPort: EventEmitter<PortStatusSnapshotEntity> = new EventEmitter<PortStatusSnapshotEntity>();
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

    formatName(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.name;
    }

    formatRunStatus(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.runStatus;
    }

    formatIn(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.input;
    }

    formatOut(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.output;
    }

    getRunStatusIcon(port: PortStatusSnapshotEntity): string {
        switch (port.portStatusSnapshot.runStatus.toLowerCase()) {
            case 'running':
                return 'fa fa-play running';
            case 'stopped':
                return 'fa fa-stop stopped';
            case 'enabled':
                return 'fa fa-flash enabled';
            case 'disabled':
                return 'icon icon-enable-false disabled';
            case 'validating':
                return 'fa fa-spin fa-circle-notch validating';
            case 'invalid':
                return 'fa fa-warning invalid';
            default:
                return '';
        }
    }

    getPortLink(port: PortStatusSnapshotEntity): string[] {
        const componentType: ComponentType =
            this._portType === 'input' ? ComponentType.InputPort : ComponentType.OutputPort;
        return ['/process-groups', port.portStatusSnapshot.groupId, componentType, port.id];
    }

    select(port: PortStatusSnapshotEntity): void {
        this.selectPort.next(port);
    }

    private selectNone() {
        this.clearSelection.next();
    }

    isSelected(port: PortStatusSnapshotEntity): boolean {
        if (this.selectedPortId) {
            return port.id === this.selectedPortId;
        }
        return false;
    }

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    canRead(port: PortStatusSnapshotEntity) {
        return port.canRead;
    }

    private supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
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

    private sortEntities(data: PortStatusSnapshotEntity[], sort: Sort): PortStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(a.portStatusSnapshot.name, b.portStatusSnapshot.name);
                    break;
                case 'runStatus':
                    retVal = this.nifiCommon.compareString(
                        a.portStatusSnapshot.runStatus,
                        b.portStatusSnapshot.runStatus
                    );
                    break;
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.flowFilesIn,
                            b.portStatusSnapshot.flowFilesIn
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.bytesIn,
                            b.portStatusSnapshot.bytesIn
                        );
                    }
                    break;
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.flowFilesOut,
                            b.portStatusSnapshot.flowFilesOut
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.bytesOut,
                            b.portStatusSnapshot.bytesOut
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
