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

import { Component, Inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Droplets } from 'apps/nifi-registry/src/app/state/droplets';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { CloseOnEscapeDialog, NiFiCommon } from '@nifi/shared';
import { MatMenuModule } from '@angular/material/menu';
import { exportFlowVersion } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { MatButtonModule } from '@angular/material/button';

interface Data {
    droplet: Droplets;
    versions: any;
}

interface VersionedFlowSnapshotMetadata {
    bucketIdentifier: string;
    flowIdentifier: string;
    version: string;
    timestamp: number;
    author: string;
    comments: string;
    branch?: string;
}

@Component({
    selector: 'app-flow-versions-dialog',
    standalone: true,
    imports: [CommonModule, MatTableModule, MatSortModule, MatDialogModule, MatMenuModule, MatButtonModule],
    templateUrl: './flow-versions-dialog.component.html',
    styleUrl: './flow-versions-dialog.component.scss'
})
export class FlowVersionsDialogComponent extends CloseOnEscapeDialog {
    dataSource: MatTableDataSource<VersionedFlowSnapshotMetadata> =
        new MatTableDataSource<VersionedFlowSnapshotMetadata>();

    sort: Sort = {
        active: 'created',
        direction: 'desc'
    };
    displayedColumns: string[] = ['version', 'created', 'comments', 'actions'];
    timeOffset = 0;

    constructor(
        @Inject(MAT_DIALOG_DATA) public data: Data,
        private nifiCommon: NiFiCommon,
        private store: Store
    ) {
        super();
        this.dataSource.data = data.versions;
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortVersions(this.dataSource.data, sort);
    }

    sortVersions(data: VersionedFlowSnapshotMetadata[], sort: Sort): VersionedFlowSnapshotMetadata[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'version':
                    retVal = this.compareVersion(a.version, b.version);
                    break;
                case 'created':
                    retVal = this.nifiCommon.compareNumber(a.timestamp, b.timestamp);
                    break;
                case 'comments':
                    retVal = this.nifiCommon.compareString(a.comments, b.comments);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    formatTimestamp(flowVersion: VersionedFlowSnapshotMetadata) {
        // get the current user time to properly convert the server time
        const now: Date = new Date();

        // convert the user offset to millis
        const userTimeOffset: number = now.getTimezoneOffset() * 60 * 1000;

        // create the proper date by adjusting by the offsets
        const date: Date = new Date(flowVersion.timestamp + userTimeOffset + this.timeOffset);
        return this.nifiCommon.formatDateTime(date);
    }

    exportVersion(version: number) {
        this.store.dispatch(exportFlowVersion({ request: { droplet: this.data.droplet, version } }));
    }

    private compareVersion(a: string, b: string): number {
        if (this.nifiCommon.isNumber(a) && this.nifiCommon.isNumber(b)) {
            return this.nifiCommon.compareNumber(parseInt(a, 10), parseInt(b, 10));
        } else {
            return this.nifiCommon.compareString(a, b);
        }
    }
}
