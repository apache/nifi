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

import { Component, OnInit } from '@angular/core';
import { selectDroplets } from '../../../state/droplets/droplets.selectors';
import {
    loadDroplets,
    openDeleteDropletDialog,
    openExportFlowVersionDialog,
    openFlowVersionsDialog,
    openImportNewFlowDialog,
    openImportNewFlowVersionDialog
} from '../../../state/droplets/droplets.actions';
import { Droplets } from '../../../state/droplets';
import { Store } from '@ngrx/store';
import { MatTableDataSource } from '@angular/material/table';
import { Observable } from 'rxjs';
import { Sort } from '@angular/material/sort';
import { NiFiCommon, selectQueryParams } from '@nifi/shared';
import { loadBuckets } from '../../../state/buckets/buckets.actions';
import { Bucket } from '../../../state/buckets';
import { selectBuckets } from '../../../state/buckets/buckets.selectors';
import { DropletTableFilterContext } from './ui/droplet-table-filter/droplet-table-filter.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
    selector: 'explorer',
    templateUrl: './explorer.component.html',
    styleUrl: './explorer.component.scss',
    standalone: false
})
export class ExplorerComponent implements OnInit {
    droplets$: Observable<Droplets[]>;
    buckets$: Observable<Bucket[]>;
    buckets: Bucket[] = [];
    dataSource: MatTableDataSource<Droplets> = new MatTableDataSource<Droplets>();
    displayedColumns: string[] = [
        'name',
        'type',
        'bucketName',
        'bucketIdentifier',
        'identifier',
        'versions',
        'actions'
    ];
    sort: Sort = {
        active: 'name',
        direction: 'asc'
    };
    filterTerm = '';
    filterBucket = 'All';
    filterColumn = '';

    constructor(
        private store: Store,
        private nifiCommon: NiFiCommon,
        private router: Router,
        private activatedRoute: ActivatedRoute
    ) {
        this.droplets$ = this.store.select(selectDroplets);
        this.buckets$ = this.store.select(selectBuckets);
        this.store
            .select(selectQueryParams)
            .pipe(takeUntilDestroyed())
            .subscribe((queryParams) => {
                if (queryParams) {
                    this.filterTerm = queryParams['filterTerm'] || '';
                    this.filterBucket = queryParams['filterBucket'] || 'All';
                    this.filterColumn = queryParams['filterColumn'] || '';
                    this.dataSource.filter = JSON.stringify(queryParams);
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadDroplets());
        this.store.dispatch(loadBuckets());
        this.droplets$.subscribe((droplets) => {
            this.dataSource.data = droplets;
            this.sortData(this.sort);
        });
        this.buckets$.subscribe((buckets) => {
            this.buckets = buckets;
        });

        this.dataSource.filterPredicate = (data: Droplets, filter: string) => {
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

            return this.nifiCommon.stringContains((data as any)[`${filterColumn}`], filterTerm, true);
        };
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortVersions(this.dataSource.data, sort);
    }

    sortVersions(data: Droplets[], sort: Sort): Droplets[] {
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

    openImportNewFlowDialog() {
        this.store.dispatch(openImportNewFlowDialog({ request: { buckets: this.buckets, activeBucket: null } }));
    }

    openImportNewFlowVersionDialog(droplet: Droplets) {
        this.store.dispatch(openImportNewFlowVersionDialog({ request: { droplet } }));
    }

    openExportFlowVersionDialog(droplet: Droplets) {
        this.store.dispatch(openExportFlowVersionDialog({ request: { droplet } }));
    }

    openDeleteDialog(droplet: Droplets) {
        this.store.dispatch(openDeleteDropletDialog({ request: { droplet } }));
    }

    openFlowVersionsDialog(droplet: Droplets) {
        this.store.dispatch(openFlowVersionsDialog({ request: { droplet } }));
    }

    applyFilter(filter: DropletTableFilterContext) {
        const { filterTerm, filterColumn, filterBucket } = filter;
        if (!filter || !this.dataSource) {
            return;
        }

        this.dataSource.filter = JSON.stringify(filter);

        this.router.navigate([], {
            relativeTo: this.activatedRoute,
            queryParams: { filterTerm, filterColumn, filterBucket },
            queryParamsHandling: 'merge'
        });
    }
}
