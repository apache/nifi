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

import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { ParameterGroupConfiguration } from '../../../../state/parameter-providers';
import { NiFiCommon } from '@nifi/shared';

@Component({
    selector: 'parameter-groups-table',
    imports: [MatSortModule, MatTableModule],
    templateUrl: './parameter-groups-table.component.html',
    styleUrls: ['./parameter-groups-table.component.scss']
})
export class ParameterGroupsTable {
    parameterGroupsDataSource: MatTableDataSource<ParameterGroupConfiguration> =
        new MatTableDataSource<ParameterGroupConfiguration>();
    selectedParameterGroup: ParameterGroupConfiguration | null = null;
    displayedColumns: string[] = ['groupName', 'indicators'];

    activeParameterGroupSort: Sort = {
        active: 'groupName',
        direction: 'asc'
    };

    constructor(private nifiCommon: NiFiCommon) {}

    @Input() set parameterGroups(parameterGroups: ParameterGroupConfiguration[]) {
        this.parameterGroupsDataSource.data = this.sortEntities(parameterGroups, this.activeParameterGroupSort);
        if (this.parameterGroupsDataSource.data.length > 0) {
            let selectedIndex = 0;
            // try to re-select the currently selected group if it still exists
            if (this.selectedParameterGroup) {
                const idx = this.parameterGroupsDataSource.data.findIndex(
                    (g) => g.groupName === this.selectedParameterGroup?.groupName
                );
                if (idx >= 0) {
                    selectedIndex = idx;
                }
            }
            this.select(this.parameterGroupsDataSource.data[selectedIndex]);
        }
    }

    @Output() selected: EventEmitter<ParameterGroupConfiguration> = new EventEmitter<ParameterGroupConfiguration>();

    sort(sort: Sort) {
        this.activeParameterGroupSort = sort;
        this.parameterGroupsDataSource.data = this.sortEntities(this.parameterGroupsDataSource.data, sort);
    }

    private sortEntities(data: ParameterGroupConfiguration[], sort: Sort) {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            const retVal = this.nifiCommon.compareString(a.groupName, b.groupName);
            return retVal * (isAsc ? 1 : -1);
        });
    }

    select(item: ParameterGroupConfiguration) {
        this.selectedParameterGroup = item;
        this.selected.next(item);
    }

    isSelected(item: ParameterGroupConfiguration): boolean {
        if (this.selectedParameterGroup) {
            return item.groupName === this.selectedParameterGroup.groupName;
        }

        return false;
    }

    isSyncedToParameterContext(item: ParameterGroupConfiguration): boolean {
        return !!item.synchronized;
    }
}
