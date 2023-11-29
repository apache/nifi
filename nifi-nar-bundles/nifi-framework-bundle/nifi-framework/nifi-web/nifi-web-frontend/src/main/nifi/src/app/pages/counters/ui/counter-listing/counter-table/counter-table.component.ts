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
import { CounterEntity } from '../../../state/counter-listing';
import { MatTableDataSource } from '@angular/material/table';
import { MatSort } from '@angular/material/sort';
import { FormBuilder, FormGroup } from '@angular/forms';
import { debounceTime } from 'rxjs';

@Component({
    selector: 'counter-table',
    templateUrl: './counter-table.component.html',
    styleUrls: ['./counter-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class CounterTable implements AfterViewInit {
    private _canModifyCounters: boolean = false;
    filterTerm: string = '';
    filterColumn: 'context' | 'name' = 'name';
    totalCount: number = 0;
    filteredCount: number = 0;

    displayedColumns: string[] = ['context', 'name', 'value'];
    dataSource: MatTableDataSource<CounterEntity> = new MatTableDataSource<CounterEntity>();
    filterForm: FormGroup;

    @Input() initialSortColumn: 'context' | 'name' = 'context';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';

    @Input() set counters(counterEntities: CounterEntity[]) {
        this.dataSource = new MatTableDataSource<CounterEntity>(counterEntities);
        this.dataSource.sort = this.sort;
        this.dataSource.sortingDataAccessor = (data: CounterEntity, displayColumn: string) => {
            switch (displayColumn) {
                case 'context':
                    return this.formatContext(data);
                case 'name':
                    return this.formatName(data);
                case 'value':
                    return data.valueCount;
                default:
                    return '';
            }
        };

        this.dataSource.filterPredicate = (data: CounterEntity, filter: string) => {
            const filterArray = filter.split('|');
            const filterTerm = filterArray[0];
            const filterColumn = filterArray[1];

            if (filterColumn === 'name') {
                return data.name.toLowerCase().indexOf(filterTerm.toLowerCase()) >= 0;
            } else {
                return data.context.toLowerCase().indexOf(filterTerm.toLowerCase()) >= 0;
            }
        };
        this.totalCount = counterEntities.length;
        this.filteredCount = counterEntities.length;

        // apply any filtering to the new data
        const filterTerm = this.filterForm.get('filterTerm')?.value;
        if (filterTerm?.length > 0) {
            const filterColumn = this.filterForm.get('filterColumn')?.value;
            this.applyFilter(filterTerm, filterColumn);
        }
    }

    @Input()
    set canModifyCounters(canWrite: boolean) {
        if (canWrite) {
            this.displayedColumns = ['context', 'name', 'value', 'reset'];
        } else {
            this.displayedColumns = ['context', 'name', 'value'];
        }
        this._canModifyCounters = canWrite;
    }

    get canModifyCounters(): boolean {
        return this._canModifyCounters;
    }

    @Output() resetCounter: EventEmitter<CounterEntity> = new EventEmitter<CounterEntity>();

    @ViewChild(MatSort) sort!: MatSort;

    constructor(private formBuilder: FormBuilder) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: 'name' });
    }

    ngAfterViewInit(): void {
        this.dataSource.sort = this.sort;

        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm.get('filterColumn')?.valueChanges.subscribe((filterColumn: string) => {
            const filterTerm = this.filterForm.get('filterTerm')?.value;
            this.applyFilter(filterTerm, filterColumn);
        });
    }

    applyFilter(filterTerm: string, filterColumn: string) {
        this.dataSource.filter = `${filterTerm}|${filterColumn}`;
        this.filteredCount = this.dataSource.filteredData.length;
    }

    formatContext(counter: CounterEntity): string {
        return counter.context;
    }

    formatName(counter: CounterEntity): string {
        return counter.name;
    }

    formatValue(counter: CounterEntity): string {
        return counter.value;
    }

    resetClicked(counter: CounterEntity, event: MouseEvent) {
        event.stopPropagation();
        this.resetCounter.next(counter);
    }
}
