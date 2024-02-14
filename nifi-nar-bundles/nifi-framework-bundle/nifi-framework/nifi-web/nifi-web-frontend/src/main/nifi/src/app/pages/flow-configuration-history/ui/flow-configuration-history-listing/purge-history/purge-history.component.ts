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

import { Component, EventEmitter, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import {
    FlowConfigurationHistoryListingState,
    PurgeHistoryRequest
} from '../../../state/flow-configuration-history-listing';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatInputModule } from '@angular/material/input';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { selectAbout } from '../../../../../state/about/about.selectors';
import { Store } from '@ngrx/store';

@Component({
    selector: 'purge-history',
    standalone: true,
    imports: [CommonModule, MatDialogModule, MatButtonModule, ReactiveFormsModule, MatInputModule, MatDatepickerModule],
    templateUrl: './purge-history.component.html',
    styleUrls: ['./purge-history.component.scss']
})
export class PurgeHistory {
    private static readonly DEFAULT_PURGE_TIME: string = '00:00:00';
    private static readonly TIME_REGEX = /^([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)$/;
    purgeHistoryForm: FormGroup;
    about$ = this.store.select(selectAbout);

    @Output() submitPurgeRequest: EventEmitter<PurgeHistoryRequest> = new EventEmitter<PurgeHistoryRequest>();

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon,
        private store: Store<FlowConfigurationHistoryListingState>
    ) {
        const now: Date = new Date();
        const aMonthAgo: Date = new Date();
        aMonthAgo.setMonth(now.getMonth() - 1);

        this.purgeHistoryForm = this.formBuilder.group({
            endDate: new FormControl(aMonthAgo, Validators.required),
            endTime: new FormControl(PurgeHistory.DEFAULT_PURGE_TIME, [
                Validators.required,
                Validators.pattern(PurgeHistory.TIME_REGEX)
            ])
        });
    }

    submit() {
        const formEndDate = this.purgeHistoryForm.get('endDate')?.value;
        const formEndTime = this.purgeHistoryForm.get('endTime')?.value;

        const request: PurgeHistoryRequest = {
            endDate: formEndDate
        };

        if (formEndDate && formEndTime) {
            const formatted = this.nifiCommon.formatDateTime(formEndDate);
            // get just the date portion because the time is entered separately by the user
            const formattedEndDateTime = formatted.split(' ');
            if (formattedEndDateTime.length > 0) {
                const formattedEndDate = formattedEndDateTime[0];

                let endTime: string = formEndTime;
                if (!endTime) {
                    endTime = PurgeHistory.DEFAULT_PURGE_TIME;
                }

                // combine the pieces into the format the api requires
                request.endDate = `${formattedEndDate} ${endTime}`;
            }

            this.submitPurgeRequest.next(request);
        }
    }
}
