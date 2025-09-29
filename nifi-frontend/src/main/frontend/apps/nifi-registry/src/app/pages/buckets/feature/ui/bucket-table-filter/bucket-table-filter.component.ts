/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, DestroyRef, EventEmitter, Input, Output, inject } from '@angular/core';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

export interface BucketTableFilterColumn {
    key: string;
    label: string;
}

export interface BucketTableFilterContext {
    filterTerm: string;
    filterColumn: string;
    changedField: string;
}

@Component({
    selector: 'bucket-table-filter',
    templateUrl: './bucket-table-filter.component.html',
    styleUrl: './bucket-table-filter.component.scss',
    standalone: true,
    imports: [ReactiveFormsModule, MatFormFieldModule, MatInputModule, MatSelectModule]
})
export class BucketTableFilterComponent {
    private formBuilder = inject(FormBuilder);
    private destroyRef = inject(DestroyRef);

    filterForm: FormGroup = this.formBuilder.group({
        filterTerm: '',
        filterColumn: ''
    });

    private _filterableColumns: BucketTableFilterColumn[] = [];

    @Input() set filterableColumns(columns: BucketTableFilterColumn[]) {
        this._filterableColumns = columns ?? [];
        if (this._filterableColumns.length > 0) {
            const current = this.filterForm.get('filterColumn')?.value;
            const valid = this._filterableColumns.some((column) => column.key === current);
            const valueToApply = valid ? current : this._filterableColumns[0].key;
            this.filterForm.get('filterColumn')?.setValue(valueToApply, { emitEvent: false });
        }
    }
    get filterableColumns(): BucketTableFilterColumn[] {
        return this._filterableColumns;
    }

    @Input() set filterTerm(term: string) {
        this.filterForm.get('filterTerm')?.setValue(term ?? '', { emitEvent: false });
    }

    @Input() set filterColumn(column: string) {
        if (column) {
            this.filterForm.get('filterColumn')?.setValue(column, { emitEvent: false });
        } else if (this._filterableColumns.length > 0) {
            this.filterForm.get('filterColumn')?.setValue(this._filterableColumns[0].key, { emitEvent: false });
        }
    }

    @Input() filteredCount = 0;
    @Input() totalCount = 0;

    @Output() filterChanged: EventEmitter<BucketTableFilterContext> = new EventEmitter<BucketTableFilterContext>();

    constructor() {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((term) => this.applyFilter(term, this.filterForm.get('filterColumn')?.value, 'filterTerm'));

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed())
            .subscribe((column) => this.applyFilter(this.filterForm.get('filterTerm')?.value, column, 'filterColumn'));
    }

    private applyFilter(filterTerm: string, filterColumn: string, changedField: string) {
        this.filterChanged.emit({
            filterTerm: filterTerm ?? '',
            filterColumn: filterColumn ?? '',
            changedField
        });
    }
}
