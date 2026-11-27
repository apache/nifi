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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NiFiCommon } from '@nifi/shared';
import { createProcessGroupDrainSummary } from '../../../../testing/process-group-drain-fixture';
import { DrainSummaryTip } from './drain-summary-tip.component';
import {
    DRAIN_NO_FLOWFILES_REMAIN,
    DRAIN_WAITING_FOR_SOURCES,
    DRAINING_DESCRIPTION,
    getDrainStatusDetail
} from '../../drain/drain-summary.utils';

describe('DrainSummaryTip', () => {
    let component: DrainSummaryTip;
    let fixture: ComponentFixture<DrainSummaryTip>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [DrainSummaryTip]
        });
        fixture = TestBed.createComponent(DrainSummaryTip);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('shows an indeterminate bar while source components are stopping', () => {
        fixture.componentRef.setInput('data', createProcessGroupDrainSummary({ prepared: false }));
        fixture.detectChanges();

        expect(component.isProgressIndeterminate()).toBe(true);
        expect(component.getStatusDetail()).toBe(DRAIN_WAITING_FOR_SOURCES);
        expect(fixture.nativeElement.textContent).toContain(DRAINING_DESCRIPTION);
        expect(fixture.nativeElement.querySelector('mat-progress-bar')).toBeTruthy();
    });

    it('shows remaining FlowFiles on a determinate bar after the drain is prepared', () => {
        const summary = createProcessGroupDrainSummary({
            prepared: true,
            originalCount: 10,
            currentCount: 4
        });
        fixture.componentRef.setInput('data', summary);
        fixture.detectChanges();

        expect(component.isProgressIndeterminate()).toBe(false);
        expect(component.getPercentCompleted()).toBe(60);
        expect(component.getStatusDetail()).toBe(getDrainStatusDetail(summary, TestBed.inject(NiFiCommon)));
    });

    it('says no FlowFiles remain once the queue is empty', () => {
        fixture.componentRef.setInput(
            'data',
            createProcessGroupDrainSummary({ prepared: true, originalCount: 10, currentCount: 0 })
        );
        fixture.detectChanges();

        expect(component.getStatusDetail()).toBe(DRAIN_NO_FLOWFILES_REMAIN);
    });
});
