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

import { Component, Input, inject } from '@angular/core';
import { MatProgressBar } from '@angular/material/progress-bar';
import { NiFiCommon, ProcessGroupDrainSummary } from '@nifi/shared';
import { DRAINING_DESCRIPTION, getDrainPercentCompleted, getDrainStatusDetail } from '../../drain/drain-summary.utils';

@Component({
    selector: 'drain-summary-tip',
    imports: [MatProgressBar],
    templateUrl: './drain-summary-tip.component.html',
    styleUrl: './drain-summary-tip.component.scss'
})
export class DrainSummaryTip {
    private nifiCommon = inject(NiFiCommon);

    @Input() data: ProcessGroupDrainSummary | undefined;

    protected readonly title = DRAINING_DESCRIPTION;

    isProgressIndeterminate(): boolean {
        return !this.data?.prepared;
    }

    getPercentCompleted(): number {
        if (!this.data) {
            return 0;
        }
        return getDrainPercentCompleted(this.data.originalCount, this.data.currentCount);
    }

    getStatusDetail(): string {
        if (!this.data) {
            return '';
        }
        return getDrainStatusDetail(this.data, this.nifiCommon);
    }
}
