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

import { AfterViewInit, Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { BaseSnapshotEntity } from '../../../state';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MultiSort } from '../index';
import { SummaryTableFilterContext } from '../summary-table-filter/summary-table-filter.component';
import { MatPaginator } from '@angular/material/paginator';
import { NodeSearchResult } from '../../../../../state/cluster-summary';

@Component({
    selector: 'component-status-table',
    imports: [MatTableModule, MatSortModule],
    template: ''
})
export abstract class ComponentStatusTable<T extends BaseSnapshotEntity> implements AfterViewInit {
    private _summaryListingStatus: string | null = null;
    private _loadedTimestamp: string | null = null;
    private _initialSortColumn!: string;
    private _initialSortDirection: SortDirection = 'asc';
    private _connectedToCluster: boolean = false;
    private _clusterNodes: NodeSearchResult[] | null = null;
    private _selectedClusterNode: NodeSearchResult | null = null;
    private _selectedId: string | null = null;

    totalCount = 0;
    filteredCount = 0;

    multiSort: MultiSort = {
        active: this._initialSortColumn,
        direction: this._initialSortDirection,
        sortValueIndex: 0,
        totalValues: 2
    };

    dataSource: MatTableDataSource<T> = new MatTableDataSource<T>();
    @ViewChild(MatPaginator) paginator!: MatPaginator;

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;
    }

    abstract sortEntities(data: T[], sort: Sort): T[];

    abstract supportsMultiValuedSort(sort: Sort): boolean;

    abstract filterPredicate(data: T, filter: string): boolean;

    applyFilter(filter: SummaryTableFilterContext) {
        if (!filter || !this.dataSource) {
            return;
        }

        // determine if the filter changing is the selected cluster node, if so a new query needs issued to the backend
        if (filter.changedField === 'clusterNode' && filter.clusterNode) {
            // need to re-issue the query with the selected cluster node id
            this.clusterNodeSelected.next(filter.clusterNode);
        }

        this.dataSource.filter = JSON.stringify(filter);
        this.filteredCount = this.dataSource.filteredData.length;
        this.resetPaginator();
        this.selectNone();
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

    @Input({}) set components(components: T[]) {
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

    @Input() set summaryListingStatus(value: string | null) {
        this._summaryListingStatus = value;
    }

    get summaryListingStatus(): string | null {
        return this._summaryListingStatus;
    }

    @Input() set connectedToCluster(value: boolean) {
        this._connectedToCluster = value;
    }

    get connectedToCluster(): boolean {
        return this._connectedToCluster;
    }

    @Input() set clusterNodes(nodes: NodeSearchResult[] | null) {
        this._clusterNodes = nodes;
    }

    get clusterNodes(): NodeSearchResult[] | null {
        return this._clusterNodes;
    }

    @Input() set selectedClusterNode(selectedClusterNode: NodeSearchResult | null) {
        this._selectedClusterNode = selectedClusterNode;
    }

    get selectedClusterNode(): NodeSearchResult | null {
        return this._selectedClusterNode;
    }

    @Input() set selectedId(selectedId: string | null) {
        this._selectedId = selectedId;
    }

    get selectedId(): string | null {
        return this._selectedId;
    }

    @Output() refresh: EventEmitter<void> = new EventEmitter<void>();
    @Output() viewStatusHistory: EventEmitter<T> = new EventEmitter<T>();
    @Output() selectComponent: EventEmitter<T> = new EventEmitter<T>();
    @Output() viewClusteredDetails: EventEmitter<T> = new EventEmitter<T>();
    @Output() clearSelection: EventEmitter<void> = new EventEmitter<void>();
    @Output() clusterNodeSelected: EventEmitter<NodeSearchResult> = new EventEmitter<NodeSearchResult>();

    resetPaginator(): void {
        if (this.dataSource.paginator) {
            this.dataSource.paginator.firstPage();
        }
    }

    paginationChanged(): void {
        // clear out any selection
        this.selectNone();
    }

    selectNone() {
        this.clearSelection.next();
    }

    sortData(sort: Sort) {
        this.setMultiSort(sort);
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    compare(a: number | string, b: number | string, isAsc: boolean) {
        return (a < b ? -1 : a > b ? 1 : 0) * (isAsc ? 1 : -1);
    }

    select(item: T) {
        this.selectComponent.next(item);
    }

    isSelected(item: T): boolean {
        if (this.selectedId) {
            return this.selectedId === item.id;
        }
        return false;
    }

    viewStatusHistoryClicked(component: T): void {
        this.viewStatusHistory.next(component);
    }

    viewClusteredDetailsClicked(component: T): void {
        this.viewClusteredDetails.next(component);
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
