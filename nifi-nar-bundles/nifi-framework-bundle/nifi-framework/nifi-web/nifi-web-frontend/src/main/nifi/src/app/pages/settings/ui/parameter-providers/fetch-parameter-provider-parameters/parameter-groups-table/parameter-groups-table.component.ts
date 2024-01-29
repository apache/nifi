import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { ParameterGroupConfiguration } from '../../../../state/parameter-providers';
import { NiFiCommon } from '../../../../../../service/nifi-common.service';

@Component({
    selector: 'parameter-groups-table',
    standalone: true,
    imports: [CommonModule, MatSortModule, MatTableModule],
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
