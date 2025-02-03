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

import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatDialogModule } from '@angular/material/dialog';
import { GarbageCollection, RepositoryStorageUsage, SystemDiagnosticsState } from '../../../state/system-diagnostics';
import { Store } from '@ngrx/store';
import {
    selectSystemDiagnostics,
    selectSystemDiagnosticsLoadedTimestamp,
    selectSystemDiagnosticsStatus
} from '../../../state/system-diagnostics/system-diagnostics.selectors';
import { MatButtonModule } from '@angular/material/button';
import { reloadSystemDiagnostics } from '../../../state/system-diagnostics/system-diagnostics.actions';
import { isDefinedAndNotNull, NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { TabbedDialog } from '../tabbed-dialog/tabbed-dialog.component';
import { ErrorContextKey } from '../../../state/error';
import { ContextErrorBanner } from '../context-error-banner/context-error-banner.component';

@Component({
    selector: 'system-diagnostics-dialog',
    imports: [
        CommonModule,
        MatTabsModule,
        MatDialogModule,
        MatButtonModule,
        NifiTooltipDirective,
        MatProgressBarModule,
        ContextErrorBanner
    ],
    templateUrl: './system-diagnostics-dialog.component.html',
    styleUrls: ['./system-diagnostics-dialog.component.scss']
})
export class SystemDiagnosticsDialog extends TabbedDialog implements OnInit {
    systemDiagnostics$ = this.store.select(selectSystemDiagnostics);
    loadedTimestamp$ = this.store.select(selectSystemDiagnosticsLoadedTimestamp);
    status$ = this.store.select(selectSystemDiagnosticsStatus);
    sortedGarbageCollections: GarbageCollection[] | null = null;

    constructor(
        private store: Store<SystemDiagnosticsState>,
        private nifiCommon: NiFiCommon
    ) {
        super('system-diagnostics-selected-index');
    }

    ngOnInit(): void {
        this.systemDiagnostics$.pipe(isDefinedAndNotNull()).subscribe((diagnostics) => {
            const sorted = diagnostics.aggregateSnapshot.garbageCollection.slice();
            sorted.sort((a, b) => {
                return this.nifiCommon.compareString(a.name, b.name);
            });
            this.sortedGarbageCollections = sorted;
        });
    }

    refreshSystemDiagnostics() {
        this.store.dispatch(
            reloadSystemDiagnostics({
                request: {
                    nodewise: false,
                    errorStrategy: 'banner'
                }
            })
        );
    }

    formatFloat(value: number): string {
        return this.nifiCommon.formatFloat(value);
    }

    getRepositoryStorageUsagePercent(repoStorage: RepositoryStorageUsage): number {
        return (repoStorage.usedSpaceBytes / repoStorage.totalSpaceBytes) * 100;
    }

    protected readonly TextTip = TextTip;
    protected readonly ErrorContextKey = ErrorContextKey;
}
