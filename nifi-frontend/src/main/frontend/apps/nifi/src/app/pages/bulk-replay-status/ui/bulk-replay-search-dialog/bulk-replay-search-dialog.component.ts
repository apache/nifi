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

import { AfterViewInit, Component, inject, OnDestroy, ViewChild } from '@angular/core';
import { animate, state, style, transition, trigger } from '@angular/animations';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { MatSelectModule } from '@angular/material/select';
import { MatOptionModule } from '@angular/material/core';
import { MatDividerModule } from '@angular/material/divider';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatTooltipModule } from '@angular/material/tooltip';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { SlicePipe } from '@angular/common';
import { SelectionModel } from '@angular/cdk/collections';
import { interval, Subject, Subscription, switchMap, takeUntil, takeWhile } from 'rxjs';
import { debounceTime, startWith, take, finalize } from 'rxjs/operators';
import { Store } from '@ngrx/store';
import { NiFiCommon } from '@nifi/shared';
import { ProvenanceService } from '../../../provenance/service/provenance.service';
import { ProvenanceRequest, SearchableField } from '../../../provenance/state/provenance-event-listing';
import { ProvenanceEventSummary, ProvenanceEvent } from '../../../../state/shared';
import { selectAbout } from '../../../../state/about/about.selectors';
import { selectClusterSummary } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { ClusterSummary } from '../../../../state/cluster-summary';

export interface BulkReplaySearchDialogData {
    processorId: string;
    processorName: string;
    groupId: string;
}

const PROCESSOR_ID_FIELD = 'ProcessorID';
const DEFAULT_START_TIME = '00:00:00';
const DEFAULT_END_TIME = '23:59:59';
const TIME_REGEX = /^([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)$/;

@Component({
    selector: 'bulk-replay-search-dialog',
    templateUrl: './bulk-replay-search-dialog.component.html',
    styleUrls: ['./bulk-replay-search-dialog.component.scss'],
    animations: [
        trigger('detailExpand', [
            state('collapsed', style({ height: '0px', minHeight: '0', overflow: 'hidden' })),
            state('expanded', style({ height: '*', overflow: 'hidden' })),
            transition('expanded <=> collapsed', animate('200ms cubic-bezier(0.4, 0.0, 0.2, 1)'))
        ])
    ],
    imports: [
        MatDialogModule,
        MatButtonModule,
        MatCheckboxModule,
        MatFormFieldModule,
        MatInputModule,
        MatDatepickerModule,
        MatTableModule,
        MatSortModule,
        MatPaginatorModule,
        MatSelectModule,
        MatOptionModule,
        MatDividerModule,
        MatProgressBarModule,
        MatTooltipModule,
        FormsModule,
        ReactiveFormsModule,
        SlicePipe
    ]
})
export class BulkReplaySearchDialog implements AfterViewInit, OnDestroy {
    private dialogData = inject<BulkReplaySearchDialogData>(MAT_DIALOG_DATA);
    private dialogRef = inject(MatDialogRef<BulkReplaySearchDialog>);
    private formBuilder = inject(FormBuilder);
    private provenanceService = inject(ProvenanceService);
    private nifiCommon = inject(NiFiCommon);
    private store = inject(Store);

    @ViewChild(MatPaginator) paginator!: MatPaginator;
    @ViewChild(MatSort) sort!: MatSort;

    readonly processorName = this.dialogData.processorName;
    readonly processorId = this.dialogData.processorId;
    readonly processorIdField = PROCESSOR_ID_FIELD;
    readonly jobNamePlaceholder = `${this.dialogData.processorName} ${new Date().toISOString().slice(0, 19)}`;

    searchableFields: SearchableField[] = [];
    searchOptionsLoaded = false;

    searchForm: FormGroup;

    // Search state
    searching = false;
    hasSearched = false;
    searchError: string | null = null;

    // Table
    readonly displayedColumns = ['select', 'eventId', 'eventTime', 'eventType', 'flowFileUuid', 'fileSize', 'actions'];
    readonly allColumns = [...this.displayedColumns, 'expandedDetail'];
    dataSource = new MatTableDataSource<ProvenanceEventSummary>();
    selection = new SelectionModel<ProvenanceEventSummary>(true, []);
    expandedEvent: ProvenanceEventSummary | null = null;

    // Lazy-loaded full event details (for attribute panel)
    loadedEventDetails: { [eventId: number]: ProvenanceEvent } = {};
    loadingEventIds: Set<number> = new Set();

    serverTimezone = '';
    clusterSummary: ClusterSummary | null = null;

    // Results filter
    readonly filterColumnOptions = ['any field', 'event id', 'event type', 'flow file uuid'];
    filterForm: FormGroup;

    private searchSubscription: Subscription | null = null;
    private activeProvenanceId: string | null = null;
    private destroy$ = new Subject<void>();

    get selectedCount(): number {
        return this.selection.selected.length;
    }

    get disconnectedNodeCount(): number {
        if (!this.clusterSummary) return 0;
        return this.clusterSummary.totalNodeCount - this.clusterSummary.connectedNodeCount;
    }

    get hasDisconnectedNodes(): boolean {
        return !!this.clusterSummary?.clustered && this.disconnectedNodeCount > 0;
    }

    get totalCount(): number {
        return this.dataSource.data.length;
    }

    get matchedCount(): number {
        return this.dataSource.filteredData.length;
    }

    constructor() {
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this.filterColumnOptions[0]
        });

        this.searchForm = this.formBuilder.group({
            jobName: new FormControl(''),
            startDate: new FormControl<Date | null>(null),
            startTime: new FormControl(DEFAULT_START_TIME, [Validators.required, Validators.pattern(TIME_REGEX)]),
            endDate: new FormControl<Date | null>(null),
            endTime: new FormControl(DEFAULT_END_TIME, [Validators.required, Validators.pattern(TIME_REGEX)]),
            minFileSize: new FormControl(''),
            maxFileSize: new FormControl(''),
            maxResults: new FormControl(1000, [Validators.required, Validators.min(1), Validators.pattern(/^\d+$/)])
        });

        // Load server timezone (used when formatting date filters)
        this.store
            .select(selectAbout)
            .pipe(take(1))
            .subscribe((about) => {
                if (about?.timezone) {
                    this.serverTimezone = about.timezone;
                }
            });

        // Track cluster state for the disconnected-node warning banner
        this.store
            .select(selectClusterSummary)
            .pipe(takeUntil(this.destroy$))
            .subscribe((cluster) => {
                this.clusterSummary = cluster;
            });

        // Load searchable fields from API, then run initial search
        this.provenanceService
            .getSearchOptions()
            .pipe(take(1))
            .subscribe({
                next: (response) => {
                    let fields: SearchableField[] = response.provenanceOptions?.searchableFields ?? [];

                    // Ensure ProcessorID is first
                    const pidIdx = fields.findIndex((f: SearchableField) => f.id === PROCESSOR_ID_FIELD);
                    if (pidIdx > 0) {
                        const [pidField] = fields.splice(pidIdx, 1);
                        fields = [pidField, ...fields];
                    } else if (pidIdx === -1) {
                        // Add it if missing
                        fields = [
                            {
                                id: PROCESSOR_ID_FIELD,
                                field: PROCESSOR_ID_FIELD,
                                label: 'Processor ID',
                                type: 'PROCESSOR_ID'
                            },
                            ...fields
                        ];
                    }

                    this.searchableFields = fields;

                    // Add a form control group per field
                    fields.forEach((field: SearchableField) => {
                        if (!this.searchForm.contains(field.id)) {
                            const isProcessorField = field.id === PROCESSOR_ID_FIELD;
                            this.searchForm.addControl(
                                field.id,
                                this.formBuilder.group({
                                    value: new FormControl({
                                        value: isProcessorField ? this.processorId : '',
                                        disabled: isProcessorField
                                    }),
                                    inverse: new FormControl({ value: false, disabled: isProcessorField })
                                })
                            );
                        }
                    });

                    this.searchOptionsLoaded = true;
                    this.runSearch();
                },
                error: () => {
                    this.searchOptionsLoaded = true;
                    this.searchError = 'Failed to load search options.';
                }
            });
    }

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;
        this.dataSource.sort = this.sort;

        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(300), takeUntil(this.destroy$))
            .subscribe(() => this.applyResultsFilter());

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntil(this.destroy$))
            .subscribe(() => this.applyResultsFilter());

        this.dataSource.sortingDataAccessor = (event: ProvenanceEventSummary, column: string) => {
            switch (column) {
                case 'eventId':
                    return event.eventId;
                case 'eventTime':
                    return new Date(event.eventTime).getTime();
                case 'eventType':
                    return event.eventType;
                case 'fileSize':
                    return event.fileSizeBytes;
                default:
                    return '';
            }
        };

        this.dataSource.filterPredicate = (event: ProvenanceEventSummary, filter: string) => {
            const { term, column } = JSON.parse(filter);
            if (!term) return true;
            if (column === 'any field') {
                return (
                    this.nifiCommon.stringContains(String(event.eventId), term, true) ||
                    this.nifiCommon.stringContains(event.eventType, term, true) ||
                    this.nifiCommon.stringContains(event.flowFileUuid, term, true)
                );
            } else if (column === 'event id') {
                return this.nifiCommon.stringContains(String(event.eventId), term, true);
            } else if (column === 'event type') {
                return this.nifiCommon.stringContains(event.eventType, term, true);
            } else if (column === 'flow file uuid') {
                return this.nifiCommon.stringContains(event.flowFileUuid, term, true);
            }
            return true;
        };
    }

    ngOnDestroy(): void {
        this.destroy$.next();
        this.destroy$.complete();
        this.cancelActiveSearch();
    }

    private cancelActiveSearch(): void {
        this.searchSubscription?.unsubscribe();
        if (this.activeProvenanceId) {
            this.provenanceService.deleteProvenanceQuery(this.activeProvenanceId).subscribe();
            this.activeProvenanceId = null;
        }
    }

    private buildProvenanceRequest(): ProvenanceRequest {
        const request: ProvenanceRequest = {
            maxResults: Number(this.searchForm.get('maxResults')?.value) || 1000,
            summarize: true,
            incrementalResults: false
        };

        // Always include the locked processor ID
        request.searchTerms = {
            [PROCESSOR_ID_FIELD]: { value: this.processorId, inverse: false }
        };

        // Add any other non-empty field values from the form
        this.searchableFields.forEach((field) => {
            if (field.id === PROCESSOR_ID_FIELD) return;
            const ctrl = this.searchForm.get(field.id);
            const value = ctrl?.get('value')?.value;
            const inverse = ctrl?.get('inverse')?.value ?? false;
            if (value) {
                request.searchTerms![field.id] = { value, inverse };
            }
        });

        // Only include dates when the user has explicitly picked one (non-null)
        const startDate: Date | null = this.searchForm.get('startDate')?.value ?? null;
        const endDate: Date | null = this.searchForm.get('endDate')?.value ?? null;

        if (startDate) {
            const combined = this.combineDateTime(startDate, this.searchForm.get('startTime')?.value);
            if (combined) request.startDate = combined;
        }

        if (endDate) {
            const combined = this.combineDateTime(endDate, this.searchForm.get('endTime')?.value);
            if (combined) request.endDate = combined;
        }

        const minFileSize = this.searchForm.get('minFileSize')?.value;
        if (minFileSize) request.minimumFileSize = minFileSize;

        const maxFileSize = this.searchForm.get('maxFileSize')?.value;
        if (maxFileSize) request.maximumFileSize = maxFileSize;

        return request;
    }

    private combineDateTime(date: Date | null, time: string): string | undefined {
        if (!date) return undefined;
        const d = new Date(date);
        d.setHours(0, 0, 0, 0);
        // nifiCommon.formatDateTime returns "MM/DD/YYYY HH:MM:SS" — take just the date part
        const datePart = this.nifiCommon.formatDateTime(d).split(' ')[0];
        // Use the server timezone if available, otherwise omit (NiFi will use server default)
        const tz = this.serverTimezone ? ` ${this.serverTimezone}` : '';
        return `${datePart} ${time || '00:00:00'}${tz}`;
    }

    runSearch(): void {
        this.cancelActiveSearch();
        this.searching = true;
        this.searchError = null;
        this.selection.clear();
        this.expandedEvent = null;

        const request = this.buildProvenanceRequest();

        this.searchSubscription = this.provenanceService
            .submitProvenanceQuery(request)
            .pipe(
                switchMap((response) => {
                    this.activeProvenanceId = response.provenance.id;
                    return interval(2000).pipe(
                        startWith(0),
                        switchMap(() => this.provenanceService.getProvenanceQuery(this.activeProvenanceId!)),
                        takeWhile((r) => !r.provenance.finished, true)
                    );
                }),
                finalize(() => {
                    this.searching = false;
                    if (this.activeProvenanceId) {
                        this.provenanceService.deleteProvenanceQuery(this.activeProvenanceId).subscribe();
                        this.activeProvenanceId = null;
                    }
                })
            )
            .subscribe({
                next: (response) => {
                    if (response.provenance.finished) {
                        this.filterForm.reset({ filterTerm: '', filterColumn: 'any field' }, { emitEvent: false });
                        this.dataSource.filter = '';
                        this.dataSource.data = response.provenance.results?.provenanceEvents ?? [];
                        this.dataSource.paginator = this.paginator;
                        this.dataSource.sort = this.sort;
                        this.paginator?.firstPage();
                        this.hasSearched = true;
                    }
                },
                error: (err) => {
                    const body = err.error;
                    this.searchError =
                        typeof body === 'string' && body
                            ? body
                            : body?.message || err.message || `Search failed (${err.status}).`;
                }
            });
    }

    applyResultsFilter(): void {
        const { filterTerm, filterColumn } = this.filterForm.getRawValue();
        this.dataSource.filter = JSON.stringify({ term: filterTerm, column: filterColumn });
        if (this.dataSource.paginator) {
            this.dataSource.paginator.firstPage();
        }
        this.selection.clear();
    }

    // Checkbox helpers
    isAllSelected(): boolean {
        const visible = this.dataSource.filteredData;
        return visible.length > 0 && visible.every((e) => this.selection.isSelected(e));
    }

    toggleAllRows(): void {
        if (this.isAllSelected()) {
            this.selection.clear();
        } else {
            this.selection.select(...this.dataSource.filteredData);
        }
    }

    // Expand/collapse row — lazy-load attributes on first expand
    toggleExpand(event: ProvenanceEventSummary, domEvent: MouseEvent): void {
        domEvent.stopPropagation();
        if (this.expandedEvent === event) {
            this.expandedEvent = null;
            return;
        }
        this.expandedEvent = event;
        if (!this.loadedEventDetails[event.eventId] && !this.loadingEventIds.has(event.eventId)) {
            this.loadingEventIds.add(event.eventId);
            this.provenanceService.getProvenanceEvent(event.eventId, event.clusterNodeId).subscribe({
                next: (response) => {
                    this.loadedEventDetails[event.eventId] = response.provenanceEvent;
                    this.loadingEventIds.delete(event.eventId);
                },
                error: () => {
                    this.loadingEventIds.delete(event.eventId);
                }
            });
        }
    }

    isExpanded(event: ProvenanceEventSummary): boolean {
        return this.expandedEvent === event;
    }

    isLoadingAttributes(event: ProvenanceEventSummary): boolean {
        return this.loadingEventIds.has(event.eventId);
    }

    startBulkReplay(): void {
        this.dialogRef.close({
            events: this.selection.selected,
            jobName: (this.searchForm.get('jobName')?.value as string)?.trim() || ''
        });
    }
}
