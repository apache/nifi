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

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, Input, Output, ViewChild } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatOptionModule } from '@angular/material/core';
import { MatButtonModule } from '@angular/material/button';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MAT_TOOLTIP_DEFAULT_OPTIONS, MatTooltipDefaultOptions, MatTooltipModule } from '@angular/material/tooltip';
import { MatDialog } from '@angular/material/dialog';
import { FormBuilder, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { asyncScheduler, debounceTime, interval, Subject, takeUntil, throttleTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { NiFiCommon, XL_DIALOG } from '@nifi/shared';
import { BulkReplayConfig, BulkReplayJobItem, BulkReplayJobSummary } from '../../state';
import { selectJobItems, selectJobs } from '../../state/bulk-replay-status.selectors';
import { cancelJob, loadJobItems, loadJobSummary } from '../../state/bulk-replay-status.actions';
import { BulkReplayJobDetailDialog } from '../bulk-replay-job-detail-dialog/bulk-replay-job-detail-dialog.component';

const tooltipDefaults: MatTooltipDefaultOptions = {
    showDelay: 500,
    hideDelay: 0,
    touchendHideDelay: 1000
};

@Component({
    selector: 'bulk-replay-job-table',
    templateUrl: './bulk-replay-job-table.component.html',
    styleUrls: ['./bulk-replay-job-table.component.scss'],
    host: { class: 'flex-1 flex flex-col min-h-0' },
    providers: [{ provide: MAT_TOOLTIP_DEFAULT_OPTIONS, useValue: tooltipDefaults }],
    imports: [
        MatTableModule,
        MatSortModule,
        MatFormFieldModule,
        MatInputModule,
        MatSelectModule,
        MatOptionModule,
        MatButtonModule,
        MatSlideToggleModule,
        MatProgressBarModule,
        MatPaginatorModule,
        MatTooltipModule,
        FormsModule,
        ReactiveFormsModule
    ]
})
export class BulkReplayJobTable implements AfterViewInit {
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);
    private destroyRef = inject(DestroyRef);
    private store = inject(Store);
    private dialog = inject(MatDialog);
    private router = inject(Router);

    private jobs$ = new Subject<BulkReplayJobSummary[]>();
    private jobItemsSnapshot: { [jobId: string]: BulkReplayJobItem[] } = {};

    @Input() set jobs(jobs: BulkReplayJobSummary[]) {
        if (jobs) this.jobs$.next(jobs);
    }

    @Input() loading: boolean = false;
    @Input() loadedTimestamp: string = 'N/A';
    @Input() connectedToCluster: boolean = true;
    @Input() clustered: boolean = false;
    @Input() config: BulkReplayConfig | null = null;

    @Output() refresh = new EventEmitter<void>();
    @Output() clearJobs = new EventEmitter<void>();
    @Output() configChanged = new EventEmitter<BulkReplayConfig>();

    editingTimeout = false;
    timeoutEditValue = '';

    @ViewChild(MatPaginator) paginator!: MatPaginator;

    displayedColumns: string[] = [
        'jobName',
        'submissionTime',
        'submittedBy',
        'processorName',
        'processorType',
        'status',
        'progress',
        'totalItems',
        'succeededItems',
        'failedItems',
        'actions'
    ];

    dataSource = new MatTableDataSource<BulkReplayJobSummary>();
    sort: Sort = { active: 'submissionTime', direction: 'desc' };

    filterForm: FormGroup;
    filterColumnOptions: string[] = ['job name', 'processor name', 'processor type', 'status', 'submitted by'];
    totalCount = 0;
    filteredCount = 0;

    constructor() {
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this.filterColumnOptions[0]
        });

        this.jobs$
            .pipe(
                throttleTime(250, asyncScheduler, { leading: true, trailing: true }),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((jobs) => this.applyJobsToTable(jobs));

        this.store
            .select(selectJobItems)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((items) => {
                this.jobItemsSnapshot = items;
            });
    }

    private applyJobsToTable(jobs: BulkReplayJobSummary[]): void {
        this.dataSource.data = this.sortJobs(jobs, this.sort);
        this.dataSource.filterPredicate = (data: BulkReplayJobSummary, filter: string) => {
            const { filterTerm, filterColumn } = JSON.parse(filter);
            switch (filterColumn) {
                case 'job name':
                    return this.nifiCommon.stringContains(data.jobName, filterTerm, true);
                case 'processor name':
                    return this.nifiCommon.stringContains(data.processorName, filterTerm, true);
                case 'processor type':
                    return this.nifiCommon.stringContains(data.processorType, filterTerm, true);
                case 'status':
                    return this.nifiCommon.stringContains(data.status, filterTerm, true);
                case 'submitted by':
                    return this.nifiCommon.stringContains(data.submittedBy, filterTerm, true);
                default:
                    return true;
            }
        };
        this.totalCount = jobs.length;
        this.filteredCount = this.dataSource.filteredData.length;

        const filterTerm = this.filterForm.get('filterTerm')?.value;
        if (filterTerm?.length > 0) {
            this.applyFilter(filterTerm, this.filterForm.get('filterColumn')?.value);
        } else {
            this.resetPaginator();
        }
    }

    readonly AUTO_REFRESH_SECONDS = 5;
    autoRefresh = true;

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;

        interval(this.AUTO_REFRESH_SECONDS * 1000)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe(() => {
                if (this.autoRefresh) this.refresh.emit();
            });

        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                this.applyFilter(filterTerm, this.filterForm.get('filterColumn')?.value);
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                this.applyFilter(this.filterForm.get('filterTerm')?.value, filterColumn);
            });
    }

    sortJobs(jobs: BulkReplayJobSummary[], sort: Sort): BulkReplayJobSummary[] {
        return jobs.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'jobName':
                    retVal = this.nifiCommon.compareString(a.jobName, b.jobName);
                    break;
                case 'submissionTime':
                    retVal = this.nifiCommon.compareString(a.submissionTime, b.submissionTime);
                    break;
                case 'submittedBy':
                    retVal = this.nifiCommon.compareString(a.submittedBy, b.submittedBy);
                    break;
                case 'processorName':
                    retVal = this.nifiCommon.compareString(a.processorName, b.processorName);
                    break;
                case 'processorType':
                    retVal = this.nifiCommon.compareString(a.processorType, b.processorType);
                    break;
                case 'status':
                    retVal = this.nifiCommon.compareString(a.status, b.status);
                    break;
                case 'totalItems':
                    retVal = this.nifiCommon.compareNumber(a.totalItems, b.totalItems);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortJobs(this.dataSource.data, sort);
    }

    applyFilter(filterTerm: string, filterColumn: string): void {
        this.dataSource.filter = JSON.stringify({ filterTerm, filterColumn });
        this.filteredCount = this.dataSource.filteredData.length;
        this.resetPaginator();
    }

    resetPaginator(): void {
        if (this.dataSource.paginator) this.dataSource.paginator.firstPage();
    }

    trackByJobId = (_index: number, job: BulkReplayJobSummary) => job.jobId;

    refreshClicked(): void {
        this.refresh.emit();
    }

    viewDetailsClicked(job: BulkReplayJobSummary): void {
        const dialogRef = this.dialog.open(BulkReplayJobDetailDialog, {
            ...XL_DIALOG,
            data: { job, items: [] }
        });

        // Subscribe before dispatching so the response isn't missed if the store
        // emits synchronously. The dispatched flag skips the initial cached snapshot.
        let dispatched = false;
        this.store
            .select(selectJobItems)
            .pipe(takeUntil(dialogRef.afterClosed()))
            .subscribe((allItems) => {
                const updated = allItems[job.jobId];
                if (dispatched) {
                    dialogRef.componentInstance.updateItems(updated ?? []);
                }
            });

        // Push live job summary updates to the open dialog (progress bar, status, countdown).
        this.store
            .select(selectJobs)
            .pipe(takeUntil(dialogRef.afterClosed()))
            .subscribe((jobs) => {
                const updated = jobs.find((j) => j.jobId === job.jobId);
                if (updated) {
                    dialogRef.componentInstance.updateJob(updated);
                }
            });

        dialogRef.componentInstance.refresh.pipe(takeUntil(dialogRef.afterClosed())).subscribe(() => {
            this.store.dispatch(loadJobSummary({ jobId: job.jobId }));
            this.store.dispatch(loadJobItems({ jobId: job.jobId }));
        });

        // Dispatch after subscriptions are set up.
        dispatched = true;
        this.store.dispatch(loadJobSummary({ jobId: job.jobId }));
        this.store.dispatch(loadJobItems({ jobId: job.jobId }));
    }

    goToProcessor(job: BulkReplayJobSummary): void {
        this.router.navigate(['/process-groups', job.groupId, 'Processor', job.processorId]);
    }

    isActive(job: BulkReplayJobSummary): boolean {
        return job.status === 'RUNNING' || job.status === 'QUEUED';
    }

    isWaitingForNode(job: BulkReplayJobSummary): boolean {
        if (!job.disconnectWaitDeadline) {
            return false;
        }
        return new Date(job.disconnectWaitDeadline).getTime() > Date.now();
    }

    cancelClicked(job: BulkReplayJobSummary): void {
        this.store.dispatch(cancelJob({ jobId: job.jobId }));
    }

    startEditTimeout(): void {
        this.timeoutEditValue = this.config?.nodeDisconnectTimeout ?? '';
        this.editingTimeout = true;
    }

    cancelEditTimeout(): void {
        this.editingTimeout = false;
    }

    saveTimeout(): void {
        const value = this.timeoutEditValue.trim();
        if (value) {
            this.configChanged.emit({ nodeDisconnectTimeout: value });
        }
        this.editingTimeout = false;
    }
}
