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

import { Component, OnDestroy, OnInit, inject } from '@angular/core';
import { selectBucketIdFromRoute, selectBuckets, selectBucketsState } from '../../../state/buckets/buckets.selectors';
import { loadBuckets, openCreateBucketDialog } from '../../../state/buckets/buckets.actions';
import { Bucket } from '../../../state/buckets';
import { Store } from '@ngrx/store';
import { MatTableDataSource } from '@angular/material/table';
import { Observable, Subject } from 'rxjs';
import { Sort } from '@angular/material/sort';
import { NiFiCommon } from '@nifi/shared';
import {
    BucketTableFilterColumn,
    BucketTableFilterContext
} from './ui/bucket-table-filter/bucket-table-filter.component';
import { ErrorContextKey } from '../../../state/error';
import { Router } from '@angular/router';

@Component({
    selector: 'buckets',
    templateUrl: './buckets.component.html',
    styleUrl: './buckets.component.scss',
    standalone: false
})
export class BucketsComponent implements OnInit, OnDestroy {
    private store = inject(Store);
    private nifiCommon = inject(NiFiCommon);
    private router = inject(Router);

    buckets$: Observable<Bucket[]> = this.store.select(selectBuckets);
    selectedBucketId$ = this.store.select(selectBucketIdFromRoute);
    dataSource: MatTableDataSource<Bucket> = new MatTableDataSource<Bucket>();
    displayedColumns: string[] = ['name', 'description', 'identifier', 'actions'];

    filterableColumns: BucketTableFilterColumn[] = [
        { key: 'name', label: 'Name' },
        { key: 'description', label: 'Description' },
        { key: 'identifier', label: 'Bucket ID' }
    ];
    sort: Sort = {
        active: 'name',
        direction: 'asc'
    };
    filterTerm = '';
    filterColumn = 'name';
    bucketsState$ = this.store.select(selectBucketsState);

    private destroy$ = new Subject<void>();

    ngOnInit(): void {
        this.store.dispatch(loadBuckets());
        this.buckets$.subscribe((buckets) => {
            this.dataSource.data = [...buckets];
            this.sortData(this.sort);
        });

        this.dataSource.filterPredicate = (data: Bucket, filter: string) => {
            if (!filter) {
                return true;
            }

            const { filterTerm, filterColumn } = JSON.parse(filter);

            if (!filterTerm) {
                return true;
            }

            const value = filterColumn ? (data as any)[filterColumn] : undefined;
            if (typeof value === 'number') {
                return value.toString().includes(filterTerm);
            }
            if (value) {
                return this.nifiCommon.stringContains(value, filterTerm, true);
            }
            // fall back to checking all string fields when column isn't set
            return Object.keys(data).some((key) => {
                const fieldValue = (data as any)[key];
                if (typeof fieldValue === 'string') {
                    return this.nifiCommon.stringContains(fieldValue, filterTerm, true);
                }
                if (typeof fieldValue === 'number') {
                    return fieldValue.toString().includes(filterTerm);
                }
                return false;
            });
        };

        this.dataSource.filter = JSON.stringify({ filterTerm: '', filterColumn: this.filterColumn });
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortBuckets(this.dataSource.data, sort);
    }

    sortBuckets(data: Bucket[], sort: Sort): Bucket[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(a.name, b.name);
                    break;
                case 'description':
                    retVal = this.nifiCommon.compareString(a.description, b.description);
                    break;
                case 'identifier':
                    retVal = this.nifiCommon.compareString(a.identifier, b.identifier);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    openCreateBucketDialog() {
        this.store.dispatch(openCreateBucketDialog());
    }

    applyFilter(filter: BucketTableFilterContext) {
        if (!filter || !this.dataSource) {
            return;
        }

        this.filterTerm = filter.filterTerm;
        this.filterColumn = filter.filterColumn;
        this.dataSource.filter = JSON.stringify(filter);
    }

    refreshBucketsListing() {
        this.store.dispatch(loadBuckets());
    }

    selectBucket(bucket: Bucket): void {
        this.router.navigate(['/buckets', bucket.identifier]);
    }

    protected readonly ErrorContextKey = ErrorContextKey;

    ngOnDestroy(): void {
        this.destroy$.next();
        this.destroy$.complete();
    }
}
