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
import { MultiSort } from '../../index';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NodeStatusSnapshot } from '../../../../state';

@Component({
    selector: 'component-cluster-table',
    standalone: true,
    imports: [MatTableModule, MatSortModule],
    template: ''
})
export abstract class ComponentClusterTable<T extends NodeStatusSnapshot> {
    private _initialSortColumn!: string;
    private _initialSortDirection: SortDirection = 'asc';

    selectedId: string | null = null;
    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    dataSource: MatTableDataSource<T> = new MatTableDataSource<T>();

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

    abstract sortEntities(data: T[], sort: Sort): T[];

    abstract supportsMultiValuedSort(sort: Sort): boolean;

    @Input({}) set components(components: T[]) {
        if (components) {
            this.dataSource.data = this.sortEntities(components, this.multiSort);
        }
    }

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    compare(a: number | string, b: number | string, isAsc: boolean) {
        return (a < b ? -1 : a > b ? 1 : 0) * (isAsc ? 1 : -1);
    }

    select(item: T) {
        this.selectedId = item.nodeId;
    }
    isSelected(item: T): boolean {
        if (this.selectedId) {
            return this.selectedId === item.nodeId;
        }
        return false;
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
