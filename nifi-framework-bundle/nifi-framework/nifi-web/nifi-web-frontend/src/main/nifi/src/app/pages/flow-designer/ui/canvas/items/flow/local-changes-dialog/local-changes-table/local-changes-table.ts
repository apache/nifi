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

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, Input, Output } from '@angular/core';
import { MatFormField, MatLabel } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NiFiCommon } from '../../../../../../../../service/nifi-common.service';
import { ComponentDifference, NavigateToComponentRequest } from '../../../../../../state/flow';
import { debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ComponentType } from '../../../../../../../../state/shared';

interface LocalChange {
    componentType: ComponentType;
    componentId: string;
    componentName: string;
    processGroupId: string;
    differenceType: string;
    difference: string;
}

@Component({
    selector: 'local-changes-table',
    standalone: true,
    imports: [MatFormField, MatInput, MatLabel, ReactiveFormsModule, MatTableModule, MatSortModule],
    templateUrl: './local-changes-table.html',
    styleUrl: './local-changes-table.scss'
})
export class LocalChangesTable implements AfterViewInit {
    private destroyRef: DestroyRef = inject(DestroyRef);
    initialSortColumn: 'componentName' | 'changeType' | 'difference' = 'componentName';
    initialSortDirection: 'asc' | 'desc' = 'asc';

    filterTerm = '';
    totalCount = 0;
    filteredCount = 0;

    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    displayedColumns: string[] = ['componentName', 'changeType', 'difference', 'actions'];
    dataSource: MatTableDataSource<LocalChange> = new MatTableDataSource<LocalChange>();
    filterForm: FormGroup;

    @Input() set differences(differences: ComponentDifference[]) {
        const localChanges: LocalChange[] = this.explodeDifferences(differences);
        this.dataSource.data = this.sortEntities(localChanges, this.activeSort);
        this.dataSource.filterPredicate = (data: LocalChange, filter: string) => {
            const { filterTerm } = JSON.parse(filter);
            // check the filter term in both the name and type columns
            return (
                this.nifiCommon.stringContains(data.componentName, filterTerm, true) ||
                this.nifiCommon.stringContains(data.differenceType, filterTerm, true)
            );
        };
        this.totalCount = localChanges.length;
        this.filteredCount = localChanges.length;

        // apply any filtering to the new data
        const filterTerm = this.filterForm.get('filterTerm')?.value;
        if (filterTerm?.length > 0) {
            this.applyFilter(filterTerm);
        }
    }

    @Output() goToChange: EventEmitter<NavigateToComponentRequest> = new EventEmitter<NavigateToComponentRequest>();

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: 'componentName' });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                this.applyFilter(filterTerm);
            });
    }

    applyFilter(filterTerm: string) {
        this.dataSource.filter = JSON.stringify({ filterTerm });
        this.filteredCount = this.dataSource.filteredData.length;
    }

    formatComponentName(item: LocalChange): string {
        return item.componentName || '';
    }

    formatChangeType(item: LocalChange): string {
        return item.differenceType;
    }

    formatDifference(item: LocalChange): string {
        return item.difference;
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    sortEntities(data: LocalChange[], sort: Sort): LocalChange[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'componentName':
                    retVal = this.nifiCommon.compareString(this.formatComponentName(a), this.formatComponentName(b));
                    break;
                case 'changeType':
                    retVal = this.nifiCommon.compareString(this.formatChangeType(a), this.formatChangeType(b));
                    break;
                case 'difference':
                    retVal = this.nifiCommon.compareString(this.formatDifference(a), this.formatDifference(b));
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    goToClicked(item: LocalChange) {
        const linkMeta: NavigateToComponentRequest = {
            id: item.componentId,
            type: item.componentType,
            processGroupId: item.processGroupId
        };
        this.goToChange.next(linkMeta);
    }

    private explodeDifferences(differences: ComponentDifference[]): LocalChange[] {
        return differences.reduce((accumulator, currentValue) => {
            const diffs = currentValue.differences.map(
                (diff) =>
                    ({
                        componentName: currentValue.componentName,
                        componentId: currentValue.componentId,
                        componentType: currentValue.componentType,
                        processGroupId: currentValue.processGroupId,
                        differenceType: diff.differenceType,
                        difference: diff.difference
                    }) as LocalChange
            );
            return [...accumulator, ...diffs];
        }, [] as LocalChange[]);
    }
}
