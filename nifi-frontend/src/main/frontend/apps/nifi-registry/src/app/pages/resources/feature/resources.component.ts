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

import { Component, OnInit, inject } from '@angular/core';
import {
    selectDropletIdFromRoute,
    selectDroplets,
    selectDropletState
} from '../../../state/droplets/droplets.selectors';
import { loadDroplets, openImportNewDropletDialog } from '../../../state/droplets/droplets.actions';
import { Droplet } from '../../../state/droplets';
import { Store } from '@ngrx/store';
import { MatTableDataSource } from '@angular/material/table';
import { Observable } from 'rxjs';
import { Sort } from '@angular/material/sort';
import { NiFiCommon } from '@nifi/shared';
import { loadBuckets } from '../../../state/buckets/buckets.actions';
import { Bucket } from '../../../state/buckets';
import { selectBuckets } from '../../../state/buckets/buckets.selectors';
import {
    DropletTableFilterColumn,
    DropletTableFilterContext
} from './ui/droplet-table-filter/droplet-table-filter.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ErrorContextKey } from '../../../state/error';
import { Router } from '@angular/router';

@Component({
    selector: 'resources',
    templateUrl: './resources.component.html',
    styleUrl: './resources.component.scss',
    standalone: false
})
export class ResourcesComponent implements OnInit {
    private store = inject(Store);
    private nifiCommon = inject(NiFiCommon);
    private router = inject(Router);

    droplets$: Observable<Droplet[]> = this.store.select(selectDroplets).pipe(takeUntilDestroyed());
    buckets$: Observable<Bucket[]> = this.store.select(selectBuckets).pipe(takeUntilDestroyed());
    buckets: Bucket[] = [];
    dataSource: MatTableDataSource<Droplet> = new MatTableDataSource<Droplet>();
    displayedColumns: string[] = [
        'name',
        'type',
        'bucketName',
        'bucketIdentifier',
        'identifier',
        'versions',
        'actions'
    ];

    filterableColumns: DropletTableFilterColumn[] = [
        { key: 'name', label: 'Name' },
        { key: 'type', label: 'Type' },
        { key: 'bucketName', label: 'Bucket' },
        { key: 'bucketIdentifier', label: 'Bucket ID' },
        { key: 'identifier', label: 'Resource ID' },
        { key: 'versionCount', label: 'Versions' }
    ];
    sort: Sort = {
        active: 'name',
        direction: 'asc'
    };
    filterTerm = '';
    filterBucket = 'All';
    filterColumn = '';
    dropletsState$ = this.store.select(selectDropletState);
    selectedDropletId$ = this.store.select(selectDropletIdFromRoute);

    ngOnInit(): void {
        this.store.dispatch(loadDroplets());
        this.store.dispatch(loadBuckets());
        this.droplets$.subscribe((droplets) => {
            this.dataSource.data = [...droplets];
            this.sortData(this.sort);
        });
        this.buckets$.subscribe((buckets) => {
            this.buckets = [...buckets];
        });

        this.dataSource.filterPredicate = (data: Droplet, filter: string) => {
            const { filterTerm, filterColumn, filterBucket } = JSON.parse(filter);
            const matchBucket: boolean = filterBucket !== 'All';

            if (!filterTerm && !filterBucket) {
                return true;
            }

            if (matchBucket) {
                if (data.bucketIdentifier !== filterBucket) {
                    return false;
                }
            }

            if (filterTerm === '') {
                return true;
            }

            const value = (data as any)[filterColumn];
            if (typeof value === 'number') {
                return value.toString().includes(filterTerm);
            }
            return this.nifiCommon.stringContains(value, filterTerm, true);
        };
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortVersions(this.dataSource.data, sort);
    }

    sortVersions(data: Droplet[], sort: Sort): Droplet[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'versions':
                    retVal = this.nifiCommon.compareNumber(a.versionCount, b.versionCount);
                    break;
                case 'name':
                    retVal = this.nifiCommon.compareString(a.name, b.name);
                    break;
                case 'bucketName':
                    retVal = this.nifiCommon.compareString(a.bucketName, b.bucketName);
                    break;
                case 'bucketIdentifier':
                    retVal = this.nifiCommon.compareString(a.bucketIdentifier, b.bucketIdentifier);
                    break;
                case 'identifier':
                    retVal = this.nifiCommon.compareString(a.identifier, b.identifier);
                    break;
                case 'type':
                    retVal = this.nifiCommon.compareString(a.type, b.type);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    filterWritableBuckets(buckets: Bucket[]): Bucket[] {
        return buckets.filter((bucket) => bucket.permissions.canWrite);
    }

    get canImportNewDroplet(): boolean {
        return this.buckets.length > 0 && this.filterWritableBuckets(this.buckets).length > 0;
    }

    openImportNewDropletDialog() {
        this.store.dispatch(openImportNewDropletDialog({ request: { buckets: this.buckets } }));
    }

    applyFilter(filter: DropletTableFilterContext) {
        if (!filter || !this.dataSource) {
            return;
        }

        this.dataSource.filter = JSON.stringify(filter);
    }

    refreshDropletsListing() {
        this.store.dispatch(loadDroplets());
        this.store.dispatch(loadBuckets());
    }

    selectDroplet(droplet: Droplet): void {
        this.router.navigate(['/explorer', droplet.identifier]);
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
