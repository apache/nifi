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
import { MatOption } from '@angular/material/autocomplete';
import { MatSelect } from '@angular/material/select';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

export interface ClusterTableFilterColumn {
    key: string;
    label: string;
}

export interface ClusterTableFilterArgs {
    filterTerm: string;
    filterColumn: string;
}

export interface ClusterTableFilterContext extends ClusterTableFilterArgs {
    changedField: string;
}

@Component({
    selector: 'cluster-table-filter',
    imports: [MatFormField, MatInput, MatLabel, MatOption, MatSelect, ReactiveFormsModule],
    templateUrl: './cluster-table-filter.component.html',
    styleUrl: './cluster-table-filter.component.scss'
})
export class ClusterTableFilter implements AfterViewInit {
    filterForm: FormGroup;
    private _filteredCount = 0;
    private _totalCount = 0;
    private _initialFilterColumn = 'name';
    private _filterableColumns: ClusterTableFilterColumn[] = [];
    private destroyRef: DestroyRef = inject(DestroyRef);

    showFilterMatchedLabel = false;

    @Input() set filterableColumns(filterableColumns: ClusterTableFilterColumn[]) {
        this._filterableColumns = filterableColumns;
    }

    get filterableColumns(): ClusterTableFilterColumn[] {
        return this._filterableColumns;
    }

    @Input() includeStatusFilter = false;

    @Input() set filterTerm(term: string) {
        this.filterForm.get('filterTerm')?.value(term);
    }

    @Input() set filterColumn(column: string) {
        this._initialFilterColumn = column;
        if (this.filterableColumns?.length > 0) {
            if (this.filterableColumns.findIndex((col) => col.key === column) >= 0) {
                this.filterForm.get('filterColumn')?.setValue(column);
            } else {
                this.filterForm.get('filterColumn')?.setValue(this.filterableColumns[0].key);
            }
        } else {
            this.filterForm.get('filterColumn')?.setValue(this._initialFilterColumn);
        }
    }

    @Input() set filteredCount(count: number) {
        this._filteredCount = count;
    }

    get filteredCount(): number {
        return this._filteredCount;
    }

    @Input() set totalCount(total: number) {
        this._totalCount = total;
    }

    get totalCount(): number {
        return this._totalCount;
    }

    @Output() filterChanged: EventEmitter<ClusterTableFilterContext> = new EventEmitter<ClusterTableFilterContext>();

    constructor(private formBuilder: FormBuilder) {
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this._initialFilterColumn || 'address'
        });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn, 'filterTerm');
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                this.applyFilter(filterTerm, filterColumn, 'filterColumn');
            });
    }

    applyFilter(filterTerm: string, filterColumn: string, changedField: string) {
        this.filterChanged.next({
            filterColumn,
            filterTerm,
            changedField
        });
        this.showFilterMatchedLabel = filterTerm?.length > 0;
    }
}
