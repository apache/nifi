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
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { AsyncPipe } from '@angular/common';
import { debounceTime, Observable, tap } from 'rxjs';
import { ProvenanceEventSummary } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { MatPaginator, MatPaginatorModule } from '@angular/material/paginator';
import { Lineage, LineageRequest } from '../../../state/lineage';
import { LineageComponent } from './lineage/lineage.component';
import { GoToProvenanceEventSourceRequest, ProvenanceEventRequest } from '../../../state/provenance-event-listing';
import { MatSliderModule } from '@angular/material/slider';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { ClusterSummary } from '../../../../../state/cluster-summary';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@Component({
    selector: 'provenance-event-table',
    standalone: true,
    templateUrl: './provenance-event-table.component.html',
    imports: [
        MatTableModule,
        MatSortModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        ReactiveFormsModule,
        RouterLink,
        NgxSkeletonLoaderModule,
        AsyncPipe,
        MatPaginatorModule,
        LineageComponent,
        MatSliderModule,
        ErrorBanner,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    styleUrls: ['./provenance-event-table.component.scss']
})
export class ProvenanceEventTable implements AfterViewInit {
    @Input() set events(events: ProvenanceEventSummary[]) {
        if (events) {
            this.dataSource.data = this.sortEvents(events, this.sort);
            this.dataSource.filterPredicate = (data: ProvenanceEventSummary, filter: string) => {
                const { filterTerm, filterColumn } = JSON.parse(filter);

                if (filterColumn === this.filterColumnOptions[0]) {
                    return this.nifiCommon.stringContains(data.componentName, filterTerm, true);
                } else if (filterColumn === this.filterColumnOptions[1]) {
                    return this.nifiCommon.stringContains(data.componentType, filterTerm, true);
                } else if (filterColumn === this.filterColumnOptions[2]) {
                    return this.nifiCommon.stringContains(data.eventType, filterTerm, true);
                } else {
                    return this.nifiCommon.stringContains(data.clusterNodeAddress, filterTerm, true);
                }
            };
            this.totalCount = events.length;
            this.filteredCount = events.length;

            // apply any filtering to the new data
            const filterTerm = this.filterForm.get('filterTerm')?.value;
            if (filterTerm?.length > 0) {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            } else {
                this.resetPaginator();
            }
        }
    }

    @Input() oldestEventAvailable!: string;
    @Input() timeOffset!: number;
    @Input() resultsMessage!: string;
    @Input() hasRequest!: boolean;
    @Input() loading!: boolean;
    @Input() loadedTimestamp!: string;

    @Input() set clusterSummary(clusterSummary: ClusterSummary) {
        if (clusterSummary?.connectedToCluster) {
            // if we're connected to the cluster add a node column if it's not already present
            if (!this.displayedColumns.includes('node')) {
                this.displayedColumns.splice(this.displayedColumns.length - 1, 0, 'node');
            }

            if (!this.filterColumnOptions.includes('node')) {
                this.filterColumnOptions.push('node');
            }
        } else {
            // if we're not connected to the cluster remove the node column if it is present
            const nodeIndex = this.displayedColumns.indexOf('node');
            if (nodeIndex > -1) {
                this.displayedColumns.splice(nodeIndex, 1);
            }

            const filterNodeIndex = this.filterColumnOptions.indexOf('node');
            if (filterNodeIndex > -1) {
                this.filterColumnOptions.splice(filterNodeIndex, 1);
            }
        }
    }

    @Input() set lineage$(lineage$: Observable<Lineage | null>) {
        this.provenanceLineage$ = lineage$.pipe(
            tap((lineage) => {
                let minMillis = -1;
                let maxMillis = -1;

                lineage?.results.nodes.forEach((node) => {
                    // ensure this event has an event time
                    if (minMillis < 0 || minMillis > node.millis) {
                        minMillis = node.millis;
                    }
                    if (maxMillis < 0 || maxMillis < node.millis) {
                        maxMillis = node.millis;
                    }
                });

                if (this.minEventTimestamp < 0 || minMillis < this.minEventTimestamp) {
                    this.minEventTimestamp = minMillis;
                }
                if (this.maxEventTimestamp < 0 || maxMillis > this.maxEventTimestamp) {
                    this.maxEventTimestamp = maxMillis;
                }

                // determine the range for the slider
                let range: number = this.maxEventTimestamp - this.minEventTimestamp;

                const binCount = 10;
                const remainder: number = range % binCount;
                if (remainder > 0) {
                    // if the range doesn't fall evenly into binCount, increase the
                    // range by the difference to ensure it does
                    this.maxEventTimestamp += binCount - remainder;
                    range = this.maxEventTimestamp - this.minEventTimestamp;
                }

                this.eventTimestampStep = range / binCount;

                this.initialEventTimestampThreshold = this.maxEventTimestamp;
                this.currentEventTimestampThreshold = this.initialEventTimestampThreshold;
            })
        );
    }

    @Output() openSearchCriteria: EventEmitter<void> = new EventEmitter<void>();
    @Output() clearRequest: EventEmitter<void> = new EventEmitter<void>();
    @Output() openEventDialog: EventEmitter<ProvenanceEventRequest> = new EventEmitter<ProvenanceEventRequest>();
    @Output() goToProvenanceEventSource: EventEmitter<GoToProvenanceEventSourceRequest> =
        new EventEmitter<GoToProvenanceEventSourceRequest>();
    @Output() resubmitProvenanceQuery: EventEmitter<void> = new EventEmitter<void>();
    @Output() queryLineage: EventEmitter<LineageRequest> = new EventEmitter<LineageRequest>();
    @Output() resetLineage: EventEmitter<void> = new EventEmitter<void>();
    @Output() clearBannerErrors: EventEmitter<void> = new EventEmitter<void>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;
    private destroyRef: DestroyRef = inject(DestroyRef);

    displayedColumns: string[] = [
        'moreDetails',
        'eventTime',
        'eventType',
        'flowFileUuid',
        'fileSize',
        'componentName',
        'componentType',
        'actions'
    ];
    dataSource: MatTableDataSource<ProvenanceEventSummary> = new MatTableDataSource<ProvenanceEventSummary>();
    selectedId: string | null = null;

    @ViewChild(MatPaginator) paginator!: MatPaginator;

    sort: Sort = {
        active: 'eventTime',
        direction: 'desc'
    };

    filterForm: FormGroup;
    filterColumnOptions: string[] = ['component name', 'component type', 'type'];
    totalCount = 0;
    filteredCount = 0;
    filterApplied = false;

    showLineage = false;
    provenanceLineage$!: Observable<Lineage | null>;
    eventId: number | null = null;

    minEventTimestamp = -1;
    maxEventTimestamp = -1;
    eventTimestampStep = 1;
    initialEventTimestampThreshold = 0;
    currentEventTimestampThreshold = 0;

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: this.filterColumnOptions[0] });
    }

    ngAfterViewInit(): void {
        this.dataSource.paginator = this.paginator;

        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.filterApplied = filterTerm.length > 0;
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

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortEvents(this.dataSource.data, sort);
    }

    sortEvents(events: ProvenanceEventSummary[], sort: Sort): ProvenanceEventSummary[] {
        const data: ProvenanceEventSummary[] = events.slice();
        return data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';

            let retVal = 0;
            switch (sort.active) {
                case 'eventTime':
                    // event ideas are increasing, so we can use this simple number for sorting purposes
                    // since we don't surface the timestamp as millis
                    retVal = this.nifiCommon.compareNumber(a.eventId, b.eventId);
                    break;
                case 'eventType':
                    retVal = this.nifiCommon.compareString(a.eventType, b.eventType);
                    break;
                case 'flowFileUuid':
                    retVal = this.nifiCommon.compareString(a.flowFileUuid, b.flowFileUuid);
                    break;
                case 'fileSize':
                    retVal = this.nifiCommon.compareNumber(a.fileSizeBytes, b.fileSizeBytes);
                    break;
                case 'componentName':
                    retVal = this.nifiCommon.compareString(a.componentName, b.componentName);
                    break;
                case 'componentType':
                    retVal = this.nifiCommon.compareString(a.componentType, b.componentType);
                    break;
                case 'node':
                    if (a.clusterNodeAddress && b.clusterNodeAddress) {
                        retVal = this.nifiCommon.compareString(a.clusterNodeAddress, b.clusterNodeAddress);
                    }
                    break;
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    applyFilter(filterTerm: string, filterColumn: string) {
        this.dataSource.filter = JSON.stringify({ filterTerm, filterColumn });
        this.filteredCount = this.dataSource.filteredData.length;
        this.resetPaginator();
    }

    resetPaginator(): void {
        if (this.dataSource.paginator) {
            this.dataSource.paginator.firstPage();
        }
    }

    clearRequestClicked(): void {
        this.clearRequest.next();
    }

    searchClicked() {
        this.openSearchCriteria.next();
    }

    viewDetailsClicked(event: ProvenanceEventSummary) {
        this.submitProvenanceEventRequest({
            eventId: event.eventId,
            clusterNodeId: event.clusterNodeId
        });
    }

    submitProvenanceEventRequest(request: ProvenanceEventRequest): void {
        this.openEventDialog.next(request);
    }

    select(event: ProvenanceEventSummary): void {
        this.selectedId = event.id;
    }

    isSelected(event: ProvenanceEventSummary): boolean {
        if (this.selectedId) {
            return event.id == this.selectedId;
        }
        return false;
    }

    supportsGoTo(event: ProvenanceEventSummary): boolean {
        if (event.groupId == null) {
            return false;
        }

        return !(event.componentId === 'Remote Output Port' || event.componentId === 'Remote Input Port');
    }

    goToClicked(event: ProvenanceEventSummary): void {
        this.goToEventSource({
            componentId: event.componentId,
            groupId: event.groupId
        });
    }

    goToEventSource(request: GoToProvenanceEventSourceRequest): void {
        this.goToProvenanceEventSource.next(request);
    }

    showLineageGraph(event: ProvenanceEventSummary): void {
        this.eventId = event.eventId;
        this.showLineage = true;

        this.clearBannerErrors.next();

        this.submitLineageQuery({
            lineageRequestType: 'FLOWFILE',
            uuid: event.flowFileUuid,
            clusterNodeId: event.clusterNodeId
        });
    }

    submitLineageQuery(request: LineageRequest): void {
        this.queryLineage.next(request);
    }

    hideLineageGraph(): void {
        this.showLineage = false;
        this.minEventTimestamp = -1;
        this.maxEventTimestamp = -1;
        this.eventTimestampStep = 1;
        this.initialEventTimestampThreshold = 0;
        this.currentEventTimestampThreshold = 0;
        this.clearBannerErrors.next();
        this.resetLineage.next();
    }

    formatLabel(value: number): string {
        // get the current user time to properly convert the server time
        const now: Date = new Date();

        // convert the user offset to millis
        const userTimeOffset: number = now.getTimezoneOffset() * 60 * 1000;

        // create the proper date by adjusting by the offsets
        const date: Date = new Date(value + userTimeOffset + this.timeOffset);
        return this.nifiCommon.formatDateTime(date);
    }

    handleInput(event: any): void {
        this.currentEventTimestampThreshold = Number(event.target.value);
    }

    refreshClicked(): void {
        this.resubmitProvenanceQuery.next();
    }
}
