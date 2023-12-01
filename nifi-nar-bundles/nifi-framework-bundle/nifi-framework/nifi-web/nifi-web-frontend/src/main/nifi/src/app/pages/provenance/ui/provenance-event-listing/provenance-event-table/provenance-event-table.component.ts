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

import { AfterViewInit, Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { NgForOf, NgIf } from '@angular/common';
import { debounceTime } from 'rxjs';
import { ProvenanceEvent, ProvenanceEventSummary } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';

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
        NgForOf,
        NgIf,
        RouterLink
    ],
    styleUrls: ['./provenance-event-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class ProvenanceEventTable implements AfterViewInit {
    @Input() set events(events: ProvenanceEventSummary[]) {
        this.dataSource = new MatTableDataSource<ProvenanceEventSummary>(events);
        this.dataSource.sort = this.sort;
        this.dataSource.filterPredicate = (data: ProvenanceEventSummary, filter: string) => {
            const filterArray = filter.split('|');
            const filterTerm = filterArray[0];
            const filterColumn = filterArray[1];

            if (filterColumn === this.filterColumnOptions[0]) {
                return data.componentName.toLowerCase().indexOf(filterTerm.toLowerCase()) >= 0;
            } else if (filterColumn === this.filterColumnOptions[1]) {
                return data.componentType.toLowerCase().indexOf(filterTerm.toLowerCase()) >= 0;
            } else {
                return data.eventType.toLowerCase().indexOf(filterTerm.toLowerCase()) >= 0;
            }
        };
        this.totalCount = events.length;
        this.filteredCount = events.length;

        // apply any filtering to the new data
        const filterTerm = this.filterForm.get('filterTerm')?.value;
        if (filterTerm?.length > 0) {
            const filterColumn = this.filterForm.get('filterColumn')?.value;
            this.applyFilter(filterTerm, filterColumn);
        }
    }
    @Input() oldestEventAvailable!: string;
    @Input() resultsMessage!: string;
    @Input() hasRequest!: boolean;

    @Output() openSearchCriteria: EventEmitter<void> = new EventEmitter<void>();
    @Output() clearRequest: EventEmitter<void> = new EventEmitter<void>();
    @Output() openEventDialog: EventEmitter<ProvenanceEventSummary> = new EventEmitter<ProvenanceEventSummary>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    // TODO - conditionally include the cluster column
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
    selectedEventId: string | null = null;

    @ViewChild(MatSort) sort!: MatSort;

    filterForm: FormGroup;
    filterTerm: string = '';
    filterColumnOptions: string[] = ['component name', 'component type', 'typ'];
    totalCount: number = 0;
    filteredCount: number = 0;

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: this.filterColumnOptions[0] });
    }

    ngAfterViewInit(): void {
        this.dataSource.sort = this.sort;

        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm.get('filterColumn')?.valueChanges.subscribe((filterColumn: string) => {
            const filterTerm = this.filterForm.get('filterTerm')?.value;
            this.applyFilter(filterTerm, filterColumn);
        });
    }

    applyFilter(filterTerm: string, filterColumn: string) {
        this.dataSource.filter = `${filterTerm}|${filterColumn}`;
        this.filteredCount = this.dataSource.filteredData.length;
    }

    clearRequestClicked(): void {
        this.clearRequest.next();
    }

    searchClicked() {
        this.openSearchCriteria.next();
    }

    viewDetailsClicked(event: ProvenanceEventSummary) {
        this.openEventDialog.next(event);
    }

    select(event: ProvenanceEventSummary): void {
        this.selectedEventId = event.id;
    }

    isSelected(event: ProvenanceEventSummary): boolean {
        if (this.selectedEventId) {
            return event.id == this.selectedEventId;
        }
        return false;
    }

    supportsGoTo(event: ProvenanceEventSummary): boolean {
        if (event.groupId == null) {
            return false;
        }

        if (event.componentId === 'Remote Output Port' || event.componentId === 'Remote Input Port') {
            return false;
        }

        return true;
    }

    getComponentLink(event: ProvenanceEventSummary): string[] {
        let link: string[];

        if (event.groupId == event.componentId) {
            link = ['/process-groups', event.componentId];
        } else if (event.componentId === 'Connection' || event.componentId === 'Load Balanced Connection') {
            link = ['/process-groups', event.groupId, 'Connection', event.componentId];
        } else if (event.componentId === 'Output Port') {
            link = ['/process-groups', event.groupId, 'OutputPort', event.componentId];
        } else {
            link = ['/process-groups', event.groupId, 'Processor', event.componentId];
        }

        return link;
    }
}
