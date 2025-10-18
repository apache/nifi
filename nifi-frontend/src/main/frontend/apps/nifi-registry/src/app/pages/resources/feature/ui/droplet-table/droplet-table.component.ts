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
import { Droplet } from 'apps/nifi-registry/src/app/state/droplets';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NiFiCommon } from '@nifi/shared';
import { MatMenuModule } from '@angular/material/menu';
import { MatButtonModule } from '@angular/material/button';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';
import { Store } from '@ngrx/store';
import {
    openDeleteDropletDialog,
    openExportDropletVersionDialog,
    openDropletVersionsDialog,
    openImportNewDropletVersionDialog
} from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';

@Component({
    selector: 'droplet-table',
    imports: [MatTableModule, MatSortModule, MatMenuModule, MatButtonModule],
    templateUrl: './droplet-table.component.html',
    styleUrl: './droplet-table.component.scss'
})
export class DropletTableComponent implements OnInit {
    private nifiCommon = inject(NiFiCommon);
    private store = inject(Store);

    @Input() buckets: Bucket[] = [];
    @Input() dataSource: MatTableDataSource<Droplet> = new MatTableDataSource<Droplet>();
    @Input() selectedId: string | null = null;

    @Output() selectDroplet: EventEmitter<Droplet> = new EventEmitter<Droplet>();

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

    ngOnInit(): void {
        this.sortData(this.sort);
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

    select(droplet: Droplet) {
        this.selectDroplet.next(droplet);
    }

    isSelected(droplet: Droplet): boolean {
        if (this.selectedId) {
            return this.selectedId === droplet.identifier;
        }
        return false;
    }

    openImportNewDropletVersionDialog(droplet: Droplet) {
        this.store.dispatch(
            openImportNewDropletVersionDialog({
                request: { droplet }
            })
        );
    }

    openExportDropletVersionDialog(droplet: Droplet) {
        this.store.dispatch(openExportDropletVersionDialog({ request: { droplet } }));
    }

    openDeleteDialog(droplet: Droplet) {
        this.store.dispatch(openDeleteDropletDialog({ request: { droplet } }));
    }

    openDropletVersionsDialog(droplet: Droplet) {
        this.store.dispatch(openDropletVersionsDialog({ request: { droplet } }));
    }

    canImportNewVersion(droplet: Droplet): boolean {
        return droplet.permissions.canWrite;
    }

    canExportVersion(droplet: Droplet): boolean {
        return droplet.permissions.canRead;
    }

    canDeleteResource(droplet: Droplet): boolean {
        return droplet.permissions.canDelete;
    }
}
