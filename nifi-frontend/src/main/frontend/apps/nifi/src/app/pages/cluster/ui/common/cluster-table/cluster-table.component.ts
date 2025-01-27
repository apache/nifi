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
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MultiSort } from '../../../../summary/ui/common';
import { ClusterTableFilterContext } from '../cluster-table-filter/cluster-table-filter.component';

@Component({
    selector: 'cluster-table',
    imports: [MatTableModule, MatSortModule],
    template: ''
})
export abstract class ClusterTable<T> {
    private _listingStatus: string | null = null;
    private _loadedTimestamp: string | null = null;
    private _initialSortColumn!: string;
    private _initialSortDirection: SortDirection = 'asc';
    private _selectedId: string | null = null;
    private _selectedRepoId: string | null = null;

    totalCount = 0;
    filteredCount = 0;

    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    dataSource: MatTableDataSource<T> = new MatTableDataSource<T>();

    abstract sortEntities(data: T[], sort: Sort): T[];

    abstract supportsMultiValuedSort(sort: Sort): boolean;

    abstract filterPredicate(item: T, filter: string): boolean;

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    @Input() set initialSortColumn(initialSortColumn: string) {
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

    @Input() set components(components: T[]) {
        if (components) {
            this.dataSource.data = this.sortEntities(components, this.multiSort);
            this.dataSource.filterPredicate = (data: T, filter: string) => this.filterPredicate(data, filter);

            this.totalCount = components.length;
            if (this.dataSource.filteredData.length > 0) {
                this.filteredCount = this.dataSource.filteredData.length;
            } else {
                this.filteredCount = components.length;
            }
        }
    }

    @Input() set loadedTimestamp(value: string | null) {
        this._loadedTimestamp = value;
    }

    get loadedTimestamp(): string | null {
        return this._loadedTimestamp;
    }

    @Input() set listingStatus(value: string | null) {
        this._listingStatus = value;
    }

    get listingStatus(): string | null {
        return this._listingStatus;
    }

    @Input() set selectedId(selectedId: string | null) {
        this._selectedId = selectedId;
    }

    get selectedId(): string | null {
        return this._selectedId;
    }

    @Input() set selectedRepositoryId(selectedRepositoryId: string | null) {
        this._selectedRepoId = selectedRepositoryId;
    }

    get selectedRepositoryId(): string | null {
        return this._selectedRepoId;
    }

    @Output() selectComponent: EventEmitter<T> = new EventEmitter<T>();
    @Output() clearSelection: EventEmitter<void> = new EventEmitter<void>();

    selectNone() {
        this.clearSelection.next();
    }

    applyFilter(filter: ClusterTableFilterContext) {
        if (!filter || !this.dataSource) {
            return;
        }

        this.dataSource.filter = JSON.stringify(filter);
        this.filteredCount = this.dataSource.filteredData.length;
        this.selectNone();
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
}
