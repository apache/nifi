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

import { Component, DestroyRef, inject, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import * as HistoryActions from '../../state/flow-configuration-history-listing/flow-configuration-history-listing.actions';
import { loadHistory } from '../../state/flow-configuration-history-listing/flow-configuration-history-listing.actions';
import {
    ActionEntity,
    FlowConfigurationHistoryListingState,
    HistoryQueryRequest
} from '../../state/flow-configuration-history-listing';
import { Store } from '@ngrx/store';
import {
    selectedHistoryItem,
    selectFlowConfigurationHistoryListingState,
    selectHistoryQuery
} from '../../state/flow-configuration-history-listing/flow-configuration-history-listing.selectors';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { initialHistoryState } from '../../state/flow-configuration-history-listing/flow-configuration-history-listing.reducer';
import { MatPaginatorModule, PageEvent } from '@angular/material/paginator';
import { FlowConfigurationHistoryTable } from './flow-configuration-history-table/flow-configuration-history-table.component';
import { Sort } from '@angular/material/sort';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { selectAbout } from '../../../../state/about/about.selectors';
import { debounceTime } from 'rxjs';
import { isDefinedAndNotNull, NiFiCommon } from '@nifi/shared';
import { MatButtonModule } from '@angular/material/button';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';

interface FilterableColumn {
    key: string;
    label: string;
}

@Component({
    selector: 'flow-configuration-history-listing',
    imports: [
        CommonModule,
        NgxSkeletonLoaderModule,
        MatPaginatorModule,
        FlowConfigurationHistoryTable,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        MatDatepickerModule,
        MatButtonModule
    ],
    templateUrl: './flow-configuration-history-listing.component.html',
    styleUrls: ['./flow-configuration-history-listing.component.scss']
})
export class FlowConfigurationHistoryListing implements OnInit, OnDestroy {
    private static readonly DEFAULT_START_TIME: string = '00:00:00';
    private static readonly DEFAULT_END_TIME: string = '23:59:59';
    private static readonly TIME_REGEX = /^([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)$/;
    private destroyRef = inject(DestroyRef);

    historyListingState$ = this.store.select(selectFlowConfigurationHistoryListingState);
    selectedHistoryId$ = this.store.select(selectedHistoryItem);
    queryRequest$ = this.store.select(selectHistoryQuery);
    about$ = this.store.select(selectAbout);
    currentUser$ = this.store.select(selectCurrentUser);

    pageSize = 50;
    queryRequest: HistoryQueryRequest = {
        count: this.pageSize,
        offset: 0,
        startDate: this.getFormattedStartDateTime(),
        endDate: this.getFormattedEndDateTime()
    };

    filterForm: FormGroup;
    filterableColumns: FilterableColumn[] = [
        { key: 'sourceId', label: 'id' },
        { key: 'userIdentity', label: 'user' }
    ];

    constructor(
        private store: Store<FlowConfigurationHistoryListingState>,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.queryRequest$
            .pipe(takeUntilDestroyed(), isDefinedAndNotNull())
            .subscribe((queryRequest) => (this.queryRequest = queryRequest));

        const now: Date = new Date();
        const twoWeeksAgo: Date = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 14);
        this.filterForm = this.formBuilder.group({
            filterTerm: '',
            filterColumn: this.filterableColumns[0].key,
            filterStartDate: new FormControl(twoWeeksAgo),
            filterStartTime: new FormControl(FlowConfigurationHistoryListing.DEFAULT_START_TIME, [
                Validators.required,
                Validators.pattern(FlowConfigurationHistoryListing.TIME_REGEX)
            ]),
            filterEndDate: new FormControl(now),
            filterEndTime: new FormControl(FlowConfigurationHistoryListing.DEFAULT_END_TIME, [
                Validators.required,
                Validators.pattern(FlowConfigurationHistoryListing.TIME_REGEX)
            ])
        });
    }

    ngOnDestroy(): void {
        this.store.dispatch(HistoryActions.resetHistoryState());
    }

    ngOnInit(): void {
        this.refresh();

        this.onFormChanges();
    }

    isInitialLoading(state: FlowConfigurationHistoryListingState): boolean {
        return state.loadedTimestamp == initialHistoryState.loadedTimestamp;
    }

    refresh() {
        this.store.dispatch(HistoryActions.loadHistory({ request: this.queryRequest }));
    }

    paginationChanged(pageEvent: PageEvent): void {
        // Initiate the call to the backend for the requested page of data
        this.store.dispatch(
            HistoryActions.loadHistory({
                request: {
                    ...this.queryRequest,
                    count: this.pageSize,
                    offset: pageEvent.pageIndex * this.pageSize
                }
            })
        );

        // clear out any selection
        this.store.dispatch(HistoryActions.clearHistorySelection());
    }

    selectionChanged(historyItem: ActionEntity) {
        if (historyItem) {
            this.store.dispatch(HistoryActions.selectHistoryItem({ request: { id: historyItem.id } }));
        }
    }

    sortChanged(sort: Sort) {
        if (this.queryRequest?.sortOrder !== sort.active || this.queryRequest.sortColumn !== sort.direction) {
            // always reset the pagination when changing sort
            this.store.dispatch(
                HistoryActions.loadHistory({
                    request: {
                        ...this.queryRequest,
                        count: this.pageSize,
                        offset: 0,
                        sortColumn: sort.active,
                        sortOrder: sort.direction ? `${sort.direction}` : 'asc'
                    }
                })
            );

            // clear out any selection
            this.store.dispatch(HistoryActions.clearHistorySelection());
        }
    }

    getPageIndex(): number {
        if (this.queryRequest.offset >= 0) {
            return this.queryRequest.offset / this.pageSize;
        } else return 0;
    }

    private onFormChanges() {
        this.filterForm.valueChanges
            .pipe(takeUntilDestroyed(this.destroyRef), debounceTime(300))
            .subscribe((formValue) => {
                if (!this.filterForm.valid) {
                    return;
                }
                // clear out any selection
                this.store.dispatch(HistoryActions.clearHistorySelection());

                // always reset the pagination state when a filter changes
                const historyRequest: HistoryQueryRequest = {
                    ...this.queryRequest,
                    offset: 0
                };

                if (formValue.filterTerm) {
                    if (formValue.filterColumn === 'sourceId') {
                        historyRequest.sourceId = formValue.filterTerm;
                        delete historyRequest.userIdentity;
                    } else {
                        historyRequest.userIdentity = formValue.filterTerm;
                        delete historyRequest.sourceId;
                    }
                } else {
                    delete historyRequest.sourceId;
                    delete historyRequest.userIdentity;
                }

                let start: Date = new Date();
                if (formValue.filterStartDate) {
                    historyRequest.startDate = this.getFormattedStartDateTime(
                        formValue.filterStartDate,
                        formValue.filterStartTime
                    );
                    start = new Date(historyRequest.startDate);
                }

                let end: Date = new Date(0);
                if (formValue.filterEndDate) {
                    historyRequest.endDate = this.getFormattedEndDateTime(
                        formValue.filterEndDate,
                        formValue.filterEndTime
                    );
                    end = new Date(historyRequest.endDate);
                }
                if (this.queryChanged(historyRequest) && start.getTime() <= end.getTime()) {
                    this.store.dispatch(
                        loadHistory({
                            request: historyRequest
                        })
                    );
                }
            });
    }

    private queryChanged(historyRequest: HistoryQueryRequest) {
        const before: HistoryQueryRequest = this.queryRequest;
        const proposed: HistoryQueryRequest = historyRequest;

        return (
            proposed.endDate !== before.endDate ||
            proposed.startDate !== before.startDate ||
            proposed.sourceId != before.sourceId ||
            proposed.userIdentity != before.userIdentity
        );
    }

    resetFilter(event: MouseEvent) {
        event.stopPropagation();
        const now = new Date();
        const twoWeeksAgo: Date = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 14);
        this.filterForm.reset({
            filterTerm: '',
            filterColumn: 'sourceId',
            filterStartTime: FlowConfigurationHistoryListing.DEFAULT_START_TIME,
            filterStartDate: twoWeeksAgo,
            filterEndTime: FlowConfigurationHistoryListing.DEFAULT_END_TIME,
            filterEndDate: now
        });
    }

    openMoreDetails(actionEntity: ActionEntity) {
        this.store.dispatch(
            HistoryActions.openMoreDetailsDialog({
                request: actionEntity
            })
        );
    }

    purgeHistoryClicked() {
        this.store.dispatch(HistoryActions.openPurgeHistoryDialog());
    }

    private getFormatDateTime(date: Date, time: string): string {
        let formatted = this.nifiCommon.formatDateTime(date);
        // get just the date portion because the time is entered separately by the user
        const formattedStartDateTime = formatted.split(' ');
        if (formattedStartDateTime.length > 0) {
            const formattedStartDate = formattedStartDateTime[0];

            // combine the pieces into the format the api requires
            formatted = `${formattedStartDate} ${time}`;
        }
        return formatted;
    }

    private getFormattedStartDateTime(date?: Date, time?: string): string {
        const now = new Date();
        const twoWeeksAgo: Date = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 14);
        const d: Date = date ? date : twoWeeksAgo;
        const t: string = time ? time : FlowConfigurationHistoryListing.DEFAULT_START_TIME;
        return this.getFormatDateTime(d, t);
    }

    private getFormattedEndDateTime(date?: Date, time?: string): string {
        const d: Date = date ? date : new Date();
        const t: string = time ? time : FlowConfigurationHistoryListing.DEFAULT_END_TIME;
        return this.getFormatDateTime(d, t);
    }
}
