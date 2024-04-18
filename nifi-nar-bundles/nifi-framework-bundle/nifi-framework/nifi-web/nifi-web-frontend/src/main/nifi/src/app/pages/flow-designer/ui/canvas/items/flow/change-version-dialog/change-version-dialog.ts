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

import { Component, EventEmitter, Inject, Output } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { MatButton } from '@angular/material/button';
import { MatCell, MatCellDef, MatColumnDef, MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatSortModule, Sort } from '@angular/material/sort';
import { VersionedFlowSnapshotMetadata } from '../../../../../../../state/shared';
import { ChangeVersionDialogRequest, VersionControlInformation } from '../../../../../state/flow';
import { NiFiCommon } from '../../../../../../../service/nifi-common.service';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';

@Component({
    selector: 'change-version-dialog',
    standalone: true,
    imports: [AsyncPipe, MatButton, MatCell, MatCellDef, MatColumnDef, MatDialogModule, MatSortModule, MatTableModule],
    templateUrl: './change-version-dialog.html',
    styleUrl: './change-version-dialog.scss'
})
export class ChangeVersionDialog {
    displayedColumns: string[] = ['version', 'created', 'comments'];
    dataSource: MatTableDataSource<VersionedFlowSnapshotMetadata> =
        new MatTableDataSource<VersionedFlowSnapshotMetadata>();
    selectedFlowVersion: VersionedFlowSnapshotMetadata | null = null;
    sort: Sort = {
        active: 'version',
        direction: 'desc'
    };
    versionControlInformation: VersionControlInformation;
    private timeOffset = this.store.selectSignal(selectTimeOffset);

    @Output() changeVersion: EventEmitter<VersionedFlowSnapshotMetadata> =
        new EventEmitter<VersionedFlowSnapshotMetadata>();

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: ChangeVersionDialogRequest,
        private nifiCommon: NiFiCommon,
        private store: Store<CanvasState>
    ) {
        const flowVersions = dialogRequest.versions.map((entity) => entity.versionedFlowSnapshotMetadata);
        const sortedFlowVersions = this.sortVersions(flowVersions, this.sort);
        this.selectedFlowVersion = sortedFlowVersions[0];
        this.dataSource.data = sortedFlowVersions;
        this.versionControlInformation = dialogRequest.versionControlInformation;
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
                    retVal = this.nifiCommon.compareNumber(a.version, b.version);
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

    select(flowVersion: VersionedFlowSnapshotMetadata): void {
        this.selectedFlowVersion = flowVersion;
    }

    isSelected(flowVersion: VersionedFlowSnapshotMetadata): boolean {
        if (this.selectedFlowVersion) {
            return flowVersion.version === this.selectedFlowVersion.version;
        }
        return false;
    }

    private getTimezoneOffset(): number {
        return this.timeOffset() || 0;
    }

    formatTimestamp(flowVersion: VersionedFlowSnapshotMetadata) {
        // get the current user time to properly convert the server time
        const now: Date = new Date();

        // convert the user offset to millis
        const userTimeOffset: number = now.getTimezoneOffset() * 60 * 1000;

        // create the proper date by adjusting by the offsets
        const date: Date = new Date(flowVersion.timestamp + userTimeOffset + this.getTimezoneOffset());
        return this.nifiCommon.formatDateTime(date);
    }

    changeFlowVersion() {
        if (this.selectedFlowVersion != null) {
            this.changeVersion.next(this.selectedFlowVersion);
        }
    }

    isSelectionValid() {
        if (!this.selectedFlowVersion) {
            return false;
        }
        return this.selectedFlowVersion.version !== this.versionControlInformation.version;
    }
}
