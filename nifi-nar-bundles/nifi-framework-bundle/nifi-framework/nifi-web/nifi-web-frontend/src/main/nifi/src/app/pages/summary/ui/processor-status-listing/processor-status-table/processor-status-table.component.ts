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
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { ProcessorStatusSnapshot, ProcessorStatusSnapshotEntity } from '../../../state/summary-listing';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import {
    SummaryTableFilterArgs,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { RouterLink } from '@angular/router';
import { SummaryTableFilterModule } from '../../common/summary-table-filter/summary-table-filter.module';
import { NgClass, NgIf } from '@angular/common';
import { ComponentType } from '../../../../../state/shared';
import { MultiSort } from '../../common';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { CurrentUser } from '../../../../../state/current-user';

export type SupportedColumns = 'name' | 'type' | 'processGroup' | 'runStatus' | 'in' | 'out' | 'readWrite' | 'tasks';

@Component({
    selector: 'processor-status-table',
    templateUrl: './processor-status-table.component.html',
    styleUrls: ['./processor-status-table.component.scss', '../../../../../../assets/styles/listing-table.scss'],
    standalone: true,
    imports: [RouterLink, SummaryTableFilterModule, MatTableModule, MatSortModule, NgClass, NgIf, MatPaginatorModule]
})
export class ProcessorStatusTable implements AfterViewInit {
    private _initialSortColumn: SupportedColumns = 'name';
    private _initialSortDirection: SortDirection = 'asc';

    filterableColumns: SummaryTableFilterColumn[] = [
        { key: 'name', label: 'name' },
        { key: 'type', label: 'type' }
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
        'type',
        'processGroup',
        'runStatus',
        'in',
        'out',
        'readWrite',
        'tasks',
        'actions'
    ];
    dataSource: MatTableDataSource<ProcessorStatusSnapshotEntity> =
        new MatTableDataSource<ProcessorStatusSnapshotEntity>();

    @ViewChild(MatPaginator) paginator!: MatPaginator;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;
    }

    applyFilter(filter: SummaryTableFilterArgs) {
        this.dataSource.filter = JSON.stringify(filter);
        this.filteredCount = this.dataSource.filteredData.length;
        this.resetPaginator();
    }

    @Input() selectedProcessorId!: string;

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

    @Input() set processors(processors: ProcessorStatusSnapshotEntity[]) {
        if (processors) {
            this.dataSource.data = this.sortEntities(processors, this.multiSort);
            this.dataSource.filterPredicate = (data: ProcessorStatusSnapshotEntity, filter: string): boolean => {
                const { filterTerm, filterColumn, filterStatus, primaryOnly } = JSON.parse(filter);
                const matchOnStatus: boolean = filterStatus !== 'All';

                if (primaryOnly) {
                    if (data.processorStatusSnapshot.executionNode !== 'PRIMARY') {
                        return false;
                    }
                }
                if (matchOnStatus) {
                    if (data.processorStatusSnapshot.runStatus !== filterStatus) {
                        return false;
                    }
                }
                if (filterTerm === '') {
                    return true;
                }

                const field: string = data.processorStatusSnapshot[
                    filterColumn as keyof ProcessorStatusSnapshot
                ] as string;
                return this.nifiCommon.stringContains(field, filterTerm, true);
            };

            this.totalCount = processors.length;
            this.filteredCount = processors.length;
        }
    }

    @Input() summaryListingStatus: string | null = null;
    @Input() loadedTimestamp: string | null = null;

    @Output() refresh: EventEmitter<void> = new EventEmitter<void>();
    @Output() viewStatusHistory: EventEmitter<ProcessorStatusSnapshotEntity> =
        new EventEmitter<ProcessorStatusSnapshotEntity>();
    @Output() selectProcessor: EventEmitter<ProcessorStatusSnapshotEntity> =
        new EventEmitter<ProcessorStatusSnapshotEntity>();

    resetPaginator(): void {
        if (this.dataSource.paginator) {
            this.dataSource.paginator.firstPage();
        }
    }

    formatName(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.name;
    }

    formatType(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.type;
    }

    formatProcessGroup(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.parentProcessGroupName;
    }

    formatRunStatus(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.runStatus;
    }

    formatIn(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.input;
    }

    formatOut(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.output;
    }

    formatReadWrite(processor: ProcessorStatusSnapshotEntity): string {
        return `${processor.processorStatusSnapshot.read} | ${processor.processorStatusSnapshot.written}`;
    }

    formatTasks(processor: ProcessorStatusSnapshotEntity): string {
        return `${processor.processorStatusSnapshot.tasks} | ${processor.processorStatusSnapshot.tasksDuration}`;
    }

    canRead(processor: ProcessorStatusSnapshotEntity): boolean {
        return processor.canRead;
    }

    getProcessorLink(processor: ProcessorStatusSnapshotEntity): string[] {
        return ['/process-groups', processor.processorStatusSnapshot.groupId, ComponentType.Processor, processor.id];
    }

    getRunStatusIcon(processor: ProcessorStatusSnapshotEntity): string {
        switch (processor.processorStatusSnapshot.runStatus.toLowerCase()) {
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

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ProcessorStatusSnapshotEntity[], sort: Sort): ProcessorStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            switch (sort.active) {
                case 'name':
                    return this.compare(this.formatName(a), this.formatName(b), isAsc);
                case 'type':
                    return this.compare(this.formatType(a), this.formatType(b), isAsc);
                case 'processGroup':
                    return this.compare(this.formatProcessGroup(a), this.formatProcessGroup(b), isAsc);
                case 'runStatus':
                    return this.compare(this.formatRunStatus(a), this.formatRunStatus(b), isAsc);
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.flowFilesIn,
                            b.processorStatusSnapshot.flowFilesIn,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.bytesIn,
                            b.processorStatusSnapshot.bytesIn,
                            isAsc
                        );
                    }
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.flowFilesOut,
                            b.processorStatusSnapshot.flowFilesOut,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.bytesOut,
                            b.processorStatusSnapshot.bytesOut,
                            isAsc
                        );
                    }
                case 'readWrite':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.bytesRead,
                            b.processorStatusSnapshot.bytesRead,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.bytesWritten,
                            b.processorStatusSnapshot.bytesWritten,
                            isAsc
                        );
                    }
                case 'tasks':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.taskCount,
                            b.processorStatusSnapshot.taskCount,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.tasksDurationNanos,
                            b.processorStatusSnapshot.tasksDurationNanos,
                            isAsc
                        );
                    }
                default:
                    return 0;
            }
        });
    }

    private compare(a: number | string, b: number | string, isAsc: boolean) {
        return (a < b ? -1 : a > b ? 1 : 0) * (isAsc ? 1 : -1);
    }

    private supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
            case 'readWrite':
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

    select(processor: ProcessorStatusSnapshotEntity): void {
        this.selectProcessor.next(processor);
    }

    isSelected(processor: ProcessorStatusSnapshotEntity): boolean {
        if (this.selectedProcessorId) {
            return processor.id === this.selectedProcessorId;
        }
        return false;
    }

    viewStatusHistoryClicked(event: MouseEvent, processor: ProcessorStatusSnapshotEntity): void {
        event.stopPropagation();
        this.viewStatusHistory.next(processor);
    }
}
