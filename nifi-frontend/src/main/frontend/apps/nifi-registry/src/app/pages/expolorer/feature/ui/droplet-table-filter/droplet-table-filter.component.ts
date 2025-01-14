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

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';

export interface DropletTableFilterColumn {
    key: string;
    label: string;
}

export interface DropletTableFilterArgs {
    filterTerm: string;
    filterColumn: string;
    filterBucket?: string;
}

export interface DropletTableFilterContext extends DropletTableFilterArgs {
    changedField: string;
}

@Component({
    selector: 'droplet-table-filter',
    standalone: true,
    imports: [CommonModule, ReactiveFormsModule, MatFormFieldModule, MatSelectModule, MatInputModule, MatButtonModule],
    templateUrl: './droplet-table-filter.component.html',
    styleUrl: './droplet-table-filter.component.scss'
})
export class DropletTableFilterComponent implements AfterViewInit {
    filterForm: FormGroup;
    private _initialFilterColumn = 'name';
    private _filterableColumns: DropletTableFilterColumn[] = [];
    private _buckets: Bucket[] = [];
    private destroyRef: DestroyRef = inject(DestroyRef);

    set filterableColumns(filterableColumns: DropletTableFilterColumn[]) {
        this._filterableColumns = filterableColumns;
    }
    get filterableColumns(): DropletTableFilterColumn[] {
        return this._filterableColumns;
    }

    @Input() set buckets(filterableColumns: Bucket[]) {
        this._buckets = filterableColumns;
    }
    get buckets(): Bucket[] {
        return this._buckets;
    }

    @Input() set filterTerm(term: string) {
        // this.filterForm.controls['filterTerm']?.value(term);
        this.filterForm.controls['filterTerm']?.setValue(term);
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

    @Input() set filterBucket(term: string) {
        this.filterForm.controls['filterBucket']?.setValue(term);
    }

    @Output() filterChanged: EventEmitter<DropletTableFilterContext> = new EventEmitter<DropletTableFilterContext>();

    constructor(private formBuilder: FormBuilder) {
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this._initialFilterColumn || 'name',
            filterBucket: 'All'
        });
        this._filterableColumns = [
            { key: 'name', label: 'name' },
            { key: 'type', label: 'type' },
            { key: 'bucketIdentifier', label: 'bucket identifier' },
            { key: 'identifier', label: 'identifier' }
        ];
    }

    ngAfterViewInit() {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                const filterBucket = this.filterForm.get('filterBucket')?.value;
                this.applyFilter(filterTerm, filterColumn, filterBucket, 'filterTerm');
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                const filterBucket = this.filterForm.get('filterBucket')?.value;
                this.applyFilter(filterTerm, filterColumn, filterBucket, 'filterColumn');
            });

        this.filterForm
            .get('filterBucket')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterBucket: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                this.applyFilter(filterTerm, filterColumn, filterBucket, 'filterBucket');
            });
    }

    applyFilter(filterTerm: string, filterColumn: string, filterBucket: string, changedField: string) {
        this.filterChanged.next({
            filterColumn,
            filterTerm,
            filterBucket,
            changedField
        });
    }
}
