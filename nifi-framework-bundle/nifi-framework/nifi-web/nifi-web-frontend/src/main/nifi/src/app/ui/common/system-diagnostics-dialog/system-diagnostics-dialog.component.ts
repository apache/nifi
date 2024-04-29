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

import { Component, OnDestroy, OnInit } from '@angular/core';
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
import { NiFiCommon } from '../../../service/nifi-common.service';
import { TextTip } from '../tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';
import { isDefinedAndNotNull } from '../../../state/shared';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { ErrorBanner } from '../error-banner/error-banner.component';
import { clearBannerErrors } from '../../../state/error/error.actions';

@Component({
    selector: 'system-diagnostics-dialog',
    standalone: true,
    imports: [
        CommonModule,
        MatTabsModule,
        MatDialogModule,
        MatButtonModule,
        NifiTooltipDirective,
        MatProgressBarModule,
        ErrorBanner
    ],
    templateUrl: './system-diagnostics-dialog.component.html',
    styleUrls: ['./system-diagnostics-dialog.component.scss']
})
export class SystemDiagnosticsDialog implements OnInit, OnDestroy {
    systemDiagnostics$ = this.store.select(selectSystemDiagnostics);
    loadedTimestamp$ = this.store.select(selectSystemDiagnosticsLoadedTimestamp);
    status$ = this.store.select(selectSystemDiagnosticsStatus);
    sortedGarbageCollections: GarbageCollection[] | null = null;

    constructor(
        private store: Store<SystemDiagnosticsState>,
        private nifiCommon: NiFiCommon
    ) {}

    ngOnInit(): void {
        this.systemDiagnostics$.pipe(isDefinedAndNotNull()).subscribe((diagnostics) => {
            const sorted = diagnostics.aggregateSnapshot.garbageCollection.slice();
            sorted.sort((a, b) => {
                return this.nifiCommon.compareString(a.name, b.name);
            });
            this.sortedGarbageCollections = sorted;
        });
    }

    ngOnDestroy(): void {
        this.store.dispatch(clearBannerErrors());
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
}
