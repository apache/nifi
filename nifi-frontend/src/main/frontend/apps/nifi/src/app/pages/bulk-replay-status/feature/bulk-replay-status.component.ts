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

import { Component, inject, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { map, take } from 'rxjs/operators';
import {
    clearAllJobs,
    clearFinishedJobs,
    clearSuccessfulJobs,
    loadConfig,
    loadJobs,
    refreshJobs,
    updateConfig
} from '../state/bulk-replay-status.actions';
import { selectConfig, selectJobs, selectLoading, selectLoadedTimestamp } from '../state/bulk-replay-status.selectors';
import { BulkReplayConfig, BulkReplayJobStatus } from '../state';
import { ClearJobsDialog, ClearJobsOption } from '../ui/clear-jobs-dialog/clear-jobs-dialog.component';
import { selectClusterSummary } from '../../../state/cluster-summary/cluster-summary.selectors';
import { isDefinedAndNotNull } from '@nifi/shared';

@Component({
    selector: 'bulk-replay-status',
    templateUrl: './bulk-replay-status.component.html',
    styleUrls: ['./bulk-replay-status.component.scss'],
    standalone: false
})
export class BulkReplayStatus implements OnInit {
    private store = inject(Store);
    private dialog = inject(MatDialog);

    jobs$ = this.store.select(selectJobs);
    loading$ = this.store.select(selectLoading);
    loadedTimestamp$ = this.store.select(selectLoadedTimestamp);
    config$ = this.store.select(selectConfig);
    connectedToCluster$ = this.store.select(selectClusterSummary).pipe(
        isDefinedAndNotNull(),
        map((cluster) => cluster.connectedToCluster)
    );
    clustered$ = this.store.select(selectClusterSummary).pipe(
        isDefinedAndNotNull(),
        map((cluster) => cluster.clustered)
    );

    ngOnInit(): void {
        this.store.dispatch(loadJobs());
        this.store.dispatch(loadConfig());
    }

    onUpdateConfig(config: BulkReplayConfig): void {
        this.store.dispatch(updateConfig({ config }));
    }

    onRefresh(): void {
        this.store.dispatch(refreshJobs());
        this.store.dispatch(loadJobs());
    }

    onClearJobs(): void {
        const dialogRef = this.dialog.open(ClearJobsDialog);
        dialogRef.afterClosed().subscribe((result: ClearJobsOption | undefined) => {
            if (!result) return;
            this.store
                .select(selectJobs)
                .pipe(take(1))
                .subscribe((jobs) => {
                    if (result === 'successful') {
                        const jobIds = jobs.filter((j) => j.status === 'COMPLETED').map((j) => j.jobId);
                        this.store.dispatch(clearSuccessfulJobs({ jobIds }));
                    } else if (result === 'finished') {
                        const finished: BulkReplayJobStatus[] = [
                            'COMPLETED',
                            'PARTIAL_SUCCESS',
                            'FAILED',
                            'CANCELLED',
                            'INTERRUPTED'
                        ];
                        const jobIds = jobs.filter((j) => finished.includes(j.status)).map((j) => j.jobId);
                        this.store.dispatch(clearFinishedJobs({ jobIds }));
                    } else if (result === 'all') {
                        const jobIds = jobs.map((j) => j.jobId);
                        this.store.dispatch(clearAllJobs({ jobIds }));
                    }
                });
        });
    }
}
