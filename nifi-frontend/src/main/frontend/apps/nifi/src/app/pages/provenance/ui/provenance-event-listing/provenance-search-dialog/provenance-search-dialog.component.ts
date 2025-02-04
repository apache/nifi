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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import {
    ProvenanceRequest,
    ProvenanceSearchDialogRequest,
    SearchableField
} from '../../../state/provenance-event-listing';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { NiFiCommon, TextTip, NifiTooltipDirective } from '@nifi/shared';
import { MatOption } from '@angular/material/autocomplete';
import { MatSelect } from '@angular/material/select';
import { CloseOnEscapeDialog, SelectOption } from '@nifi/shared';

@Component({
    selector: 'provenance-search-dialog',
    templateUrl: './provenance-search-dialog.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatDatepickerModule,
        MatOption,
        MatSelect,
        NifiTooltipDirective
    ],
    styleUrls: ['./provenance-search-dialog.component.scss']
})
export class ProvenanceSearchDialog extends CloseOnEscapeDialog {
    @Input() timezone!: string;

    @Output() submitSearchCriteria: EventEmitter<ProvenanceRequest> = new EventEmitter<ProvenanceRequest>();

    public static readonly MAX_RESULTS: number = 1000;
    private static readonly DEFAULT_START_TIME: string = '00:00:00';
    private static readonly DEFAULT_END_TIME: string = '23:59:59';
    private static readonly TIME_REGEX = /^([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)$/;

    provenanceOptionsForm: FormGroup;
    searchLocationOptions: SelectOption[] = [];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: ProvenanceSearchDialogRequest,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        super();
        const now = new Date();
        this.clearTime(now);

        let startDate: Date = now;
        let startTime: string = ProvenanceSearchDialog.DEFAULT_START_TIME;
        let endDate: Date = now;
        let endTime: string = ProvenanceSearchDialog.DEFAULT_END_TIME;
        let minFileSize = '';
        let maxFileSize = '';

        if (request.currentRequest) {
            const requestedStartDate = request.currentRequest.startDate;
            if (requestedStartDate) {
                startDate = this.nifiCommon.parseDateTime(requestedStartDate);
                this.clearTime(startDate);

                const requestedStartDateTime = requestedStartDate?.split(' ');
                if (requestedStartDateTime && requestedStartDateTime.length > 1) {
                    startTime = requestedStartDateTime[1];
                }
            }

            const requestedEndDate = request.currentRequest.endDate;
            if (requestedEndDate) {
                endDate = this.nifiCommon.parseDateTime(requestedEndDate);
                this.clearTime(endDate);

                const requestedEndDateTime = requestedEndDate?.split(' ');
                if (requestedEndDateTime && requestedEndDateTime.length > 0) {
                    endTime = requestedEndDateTime[1];
                }
            }

            if (request.currentRequest.minimumFileSize) {
                minFileSize = request.currentRequest.minimumFileSize;
            }

            if (request.currentRequest.maximumFileSize) {
                maxFileSize = request.currentRequest.maximumFileSize;
            }
        }

        this.provenanceOptionsForm = this.formBuilder.group({
            startDate: new FormControl(startDate),
            startTime: new FormControl(startTime, [
                Validators.required,
                Validators.pattern(ProvenanceSearchDialog.TIME_REGEX)
            ]),
            endDate: new FormControl(endDate),
            endTime: new FormControl(endTime, [
                Validators.required,
                Validators.pattern(ProvenanceSearchDialog.TIME_REGEX)
            ]),
            minFileSize: new FormControl(minFileSize),
            maxFileSize: new FormControl(maxFileSize)
        });

        const searchTerms: any = request.currentRequest?.searchTerms;

        const searchableFields: SearchableField[] = request.options.searchableFields;
        searchableFields.forEach((searchableField) => {
            let value = '';
            let inverse = false;
            if (searchTerms && searchTerms[searchableField.id]) {
                value = searchTerms[searchableField.id].value;
                inverse = searchTerms[searchableField.id].inverse;
            }

            this.provenanceOptionsForm.addControl(
                searchableField.id,
                this.formBuilder.group({
                    value: new FormControl(value),
                    inverse: new FormControl(inverse)
                })
            );
        });

        if (request.clusterNodes.length > 0) {
            this.searchLocationOptions = [
                {
                    text: 'cluster',
                    value: null
                }
            ];

            const sortedNodes = [...this.request.clusterNodes];
            sortedNodes.sort((a, b) => {
                return this.nifiCommon.compareString(a.address, b.address);
            });

            this.searchLocationOptions.push(
                ...sortedNodes.map((node) => {
                    return {
                        text: node.address,
                        value: node.id
                    };
                })
            );

            this.provenanceOptionsForm.addControl('searchLocation', new FormControl(null));
        }
    }

    private clearTime(date: Date): void {
        date.setHours(0);
        date.setMinutes(0);
        date.setSeconds(0);
        date.setMilliseconds(0);
    }

    searchClicked() {
        const provenanceRequest: ProvenanceRequest = {
            maxResults: ProvenanceSearchDialog.MAX_RESULTS,
            summarize: true,
            incrementalResults: false
        };

        const startDate: Date = this.provenanceOptionsForm.get('startDate')?.value;
        if (startDate) {
            // convert the date object into the format the api expects
            const formatted: string = this.nifiCommon.formatDateTime(startDate);

            // get just the date portion because the time is entered separately by the user
            const formattedStartDateTime = formatted.split(' ');
            if (formattedStartDateTime.length > 0) {
                const formattedStartDate = formattedStartDateTime[0];

                let startTime: string = this.provenanceOptionsForm.get('startTime')?.value;
                if (!startTime) {
                    startTime = ProvenanceSearchDialog.DEFAULT_START_TIME;
                }

                // combine all three pieces into the format the api requires
                provenanceRequest.startDate = `${formattedStartDate} ${startTime} ${this.timezone}`;
            }
        }

        const endDate: Date = this.provenanceOptionsForm.get('endDate')?.value;
        if (endDate) {
            // convert the date object into the format the api expects
            const formatted: string = this.nifiCommon.formatDateTime(endDate);

            // get just the date portion because the time is entered separately by the user
            const formattedEndDateTime = formatted.split(' ');
            if (formattedEndDateTime.length > 0) {
                const formattedEndDate = formattedEndDateTime[0];

                let endTime: string = this.provenanceOptionsForm.get('endTime')?.value;
                if (!endTime) {
                    endTime = ProvenanceSearchDialog.DEFAULT_END_TIME;
                }

                // combine all three pieces into the format the api requires
                provenanceRequest.endDate = `${formattedEndDate} ${endTime} ${this.timezone}`;
            }
        }

        const minFileSize: string = this.provenanceOptionsForm.get('minFileSize')?.value;
        if (minFileSize) {
            provenanceRequest.minimumFileSize = minFileSize;
        }

        const maxFileSize: string = this.provenanceOptionsForm.get('maxFileSize')?.value;
        if (maxFileSize) {
            provenanceRequest.maximumFileSize = maxFileSize;
        }

        const searchTerms: any = {};
        const searchableFields: SearchableField[] = this.request.options.searchableFields;
        searchableFields.forEach((searchableField) => {
            // @ts-ignore
            const searchableFieldForm: FormGroup = this.provenanceOptionsForm.get([searchableField.id]);
            if (searchableFieldForm) {
                const searchableFieldValue: string = searchableFieldForm.get('value')?.value;
                if (searchableFieldValue) {
                    searchTerms[searchableField.id] = {
                        value: searchableFieldValue,
                        inverse: searchableFieldForm.get('inverse')?.value
                    };
                }
            }
        });
        provenanceRequest.searchTerms = searchTerms;

        if (this.searchLocationOptions.length > 0) {
            const searchLocation = this.provenanceOptionsForm.get('searchLocation')?.value;
            if (searchLocation) {
                provenanceRequest.clusterNodeId = searchLocation;
            }
        }

        this.submitSearchCriteria.next(provenanceRequest);
    }

    protected readonly TextTip = TextTip;

    override isDirty(): boolean {
        return this.provenanceOptionsForm.dirty;
    }
}
