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

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, NgZone, Output, ViewChild } from '@angular/core';
import { interval } from 'rxjs';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatDividerModule } from '@angular/material/divider';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatOptionModule } from '@angular/material/core';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatSort, MatSortModule, Sort } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { FormBuilder, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { DatePipe } from '@angular/common';
import { Router, RouterLink } from '@angular/router';
import { debounceTime } from 'rxjs/operators';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NiFiCommon } from '@nifi/shared';
import { BulkReplayJobItem, BulkReplayJobSummary } from '../../state';

export interface BulkReplayJobDetailDialogData {
    job: BulkReplayJobSummary;
    items: BulkReplayJobItem[];
}

@Component({
    selector: 'bulk-replay-job-detail-dialog',
    templateUrl: './bulk-replay-job-detail-dialog.component.html',
    styleUrls: ['./bulk-replay-job-detail-dialog.component.scss'],
    imports: [
        MatDialogModule,
        MatButtonModule,
        MatDividerModule,
        MatFormFieldModule,
        MatInputModule,
        MatSelectModule,
        MatOptionModule,
        MatProgressBarModule,
        MatSortModule,
        MatTableModule,
        MatPaginatorModule,
        MatTooltipModule,
        ReactiveFormsModule,
        FormsModule,
        RouterLink,
        DatePipe,
        MatSlideToggleModule
    ]
})
export class BulkReplayJobDetailDialog implements AfterViewInit {
    private dialogData = inject<BulkReplayJobDetailDialogData>(MAT_DIALOG_DATA);
    private router = inject(Router);
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);
    private destroyRef = inject(DestroyRef);
    private ngZone = inject(NgZone);

    @Output() refresh = new EventEmitter<void>();

    readonly AUTO_REFRESH_SECONDS = 5;
    autoRefresh = true;
    refreshing = false;
    loadingItems = true;
    lastRefreshedAt: Date | null = null;
    countdownSeconds: number | null = null;

    @ViewChild(MatPaginator) paginator!: MatPaginator;
    @ViewChild(MatSort) matSort!: MatSort;

    job: BulkReplayJobSummary = this.dialogData.job;

    itemsDataSource = new MatTableDataSource<BulkReplayJobItem>(this.dialogData.items ?? []);

    itemColumns: string[] = [
        'provenanceEventId',
        'flowFileUuid',
        'eventTime',
        'fileSize',
        'status',
        'errorMessage',
        'actions'
    ];

    sort: Sort = { active: 'eventTime', direction: 'asc' };

    filterForm: FormGroup;
    filterColumnOptions: string[] = ['any field', 'event id', 'flow file uuid', 'status', 'error message'];
    totalCount = this.dialogData.items?.length ?? 0;
    filteredCount = this.dialogData.items?.length ?? 0;

    constructor() {
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this.filterColumnOptions[0]
        });

        this.itemsDataSource.filterPredicate = (data: BulkReplayJobItem, filter: string) => {
            const { filterTerm, filterColumn } = JSON.parse(filter);
            if (filterColumn === 'any field') {
                return (
                    this.nifiCommon.stringContains(String(data.provenanceEventId), filterTerm, true) ||
                    this.nifiCommon.stringContains(data.flowFileUuid, filterTerm, true) ||
                    this.nifiCommon.stringContains(data.eventTime, filterTerm, true) ||
                    this.nifiCommon.stringContains(data.status, filterTerm, true) ||
                    this.nifiCommon.stringContains(data.errorMessage ?? '', filterTerm, true)
                );
            } else if (filterColumn === 'event id') {
                return this.nifiCommon.stringContains(String(data.provenanceEventId), filterTerm, true);
            } else if (filterColumn === 'flow file uuid') {
                return this.nifiCommon.stringContains(data.flowFileUuid, filterTerm, true);
            } else if (filterColumn === 'status') {
                return this.nifiCommon.stringContains(data.status, filterTerm, true);
            } else if (filterColumn === 'error message') {
                return this.nifiCommon.stringContains(data.errorMessage ?? '', filterTerm, true);
            }
            return true;
        };
    }

    get jobShortId(): string {
        return this.job.jobId.substring(0, 8);
    }

    ngAfterViewInit(): void {
        this.itemsDataSource.paginator = this.paginator;
        this.itemsDataSource.sort = this.matSort;

        interval(this.AUTO_REFRESH_SECONDS * 1000)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe(() => {
                if (this.autoRefresh) {
                    this.refreshing = true;
                    this.refresh.emit();
                }
            });

        // Tick the countdown every second outside Angular's zone to avoid per-second
        // change detection on the whole dialog. Re-enters the zone only on value change.
        this.ngZone.runOutsideAngular(() => {
            interval(1000)
                .pipe(takeUntilDestroyed(this.destroyRef))
                .subscribe(() => {
                    const next = this.computeCountdownSeconds();
                    if (next !== this.countdownSeconds) {
                        this.ngZone.run(() => {
                            this.countdownSeconds = next;
                        });
                    }
                });
        });

        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });
    }

    applyFilter(filterTerm: string, filterColumn: string): void {
        this.itemsDataSource.filter = JSON.stringify({ filterTerm, filterColumn });
        this.filteredCount = this.itemsDataSource.filteredData.length;
        if (this.itemsDataSource.paginator) {
            this.itemsDataSource.paginator.firstPage();
        }
    }

    sortData(sort: Sort): void {
        this.sort = sort;
        const data = this.itemsDataSource.data.slice();
        if (!sort.active || sort.direction === '') {
            return;
        }
        this.itemsDataSource.data = data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            switch (sort.active) {
                case 'provenanceEventId':
                    return this.nifiCommon.compareNumber(a.provenanceEventId, b.provenanceEventId) * (isAsc ? 1 : -1);
                case 'eventTime':
                    return this.nifiCommon.compareString(a.eventTime, b.eventTime) * (isAsc ? 1 : -1);
                case 'status':
                    return this.nifiCommon.compareString(a.status, b.status) * (isAsc ? 1 : -1);
                case 'fileSize':
                    return this.nifiCommon.compareNumber(a.fileSizeBytes ?? 0, b.fileSizeBytes ?? 0) * (isAsc ? 1 : -1);
                case 'errorMessage':
                    return this.nifiCommon.compareString(a.errorMessage ?? '', b.errorMessage ?? '') * (isAsc ? 1 : -1);
                default:
                    return 0;
            }
        });
    }

    updateItems(items: BulkReplayJobItem[]): void {
        this.itemsDataSource.data = items ?? [];
        this.totalCount = items?.length ?? 0;
        this.filteredCount = this.itemsDataSource.filteredData.length;
        this.loadingItems = false;
        this.refreshing = false;
        this.lastRefreshedAt = new Date();
    }

    updateJob(job: BulkReplayJobSummary): void {
        this.job = job;
    }

    private computeCountdownSeconds(): number | null {
        if (!this.job.disconnectWaitDeadline) {
            return null;
        }
        const deadlineMs = new Date(this.job.disconnectWaitDeadline).getTime();
        if (isNaN(deadlineMs)) {
            return null;
        }
        const remainingMs = deadlineMs - Date.now();
        return remainingMs > 0 ? Math.ceil(remainingMs / 1000) : 0;
    }

    onManualRefresh(): void {
        this.refreshing = true;
        this.refresh.emit();
    }

    isFailed(item: BulkReplayJobItem): boolean {
        return item.status === 'FAILED';
    }

    formatFileSize(bytes: number | undefined): string {
        if (bytes == null || bytes === 0) return '0 bytes';
        return this.nifiCommon.formatDataSize(bytes);
    }

    searchProvenance(flowFileUuid: string): void {
        this.router.navigate(['/provenance'], { queryParams: { flowFileUuid } });
    }
}
