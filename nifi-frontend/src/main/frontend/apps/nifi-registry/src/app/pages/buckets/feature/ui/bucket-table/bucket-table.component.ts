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

import { Component, EventEmitter, Input, OnInit, Output, inject } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NiFiCommon } from '@nifi/shared';
import { MatMenuModule } from '@angular/material/menu';
import { MatButtonModule } from '@angular/material/button';
import { Store } from '@ngrx/store';
import {
    openDeleteBucketDialog,
    openEditBucketDialog,
    openManageBucketPoliciesDialog
} from 'apps/nifi-registry/src/app/state/buckets/buckets.actions';
import { MatIconModule } from '@angular/material/icon';

@Component({
    selector: 'bucket-table',
    standalone: true,
    imports: [MatTableModule, MatSortModule, MatMenuModule, MatButtonModule, MatIconModule],
    templateUrl: './bucket-table.component.html',
    styleUrl: './bucket-table.component.scss'
})
export class BucketTableComponent implements OnInit {
    private nifiCommon = inject(NiFiCommon);
    private store = inject(Store);

    @Input() dataSource: MatTableDataSource<Bucket> = new MatTableDataSource<Bucket>();
    @Input() selectedId: string | null = null;

    @Output() selectBucket: EventEmitter<Bucket> = new EventEmitter<Bucket>();

    displayedColumns: string[] = ['name', 'description', 'identifier', 'actions'];
    sort: Sort = {
        active: 'name',
        direction: 'asc'
    };

    ngOnInit(): void {
        this.sortData(this.sort);
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
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    select(bucket: Bucket) {
        this.selectBucket.next(bucket);
    }

    isSelected(bucket: Bucket): boolean {
        if (this.selectedId) {
            return this.selectedId === bucket.identifier;
        }
        return false;
    }

    openEditBucketDialog(bucket: Bucket) {
        this.store.dispatch(
            openEditBucketDialog({
                request: { bucket }
            })
        );
    }

    openDeleteBucketDialog(bucket: Bucket) {
        this.store.dispatch(openDeleteBucketDialog({ request: { bucket } }));
    }

    openManageBucketPoliciesDialog(bucket: Bucket) {
        this.store.dispatch(openManageBucketPoliciesDialog({ request: { bucket } }));
    }
}
