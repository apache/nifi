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

import { Component, EventEmitter, inject, Input, Output, Type } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { MatProgressBar } from '@angular/material/progress-bar';
import { Observable, of } from 'rxjs';
import {
    DrainBlockingComponent,
    ErrorBanner,
    NiFiCommon,
    NifiTooltipDirective,
    ProcessGroupDrainRequest,
    StatusBanner,
    StatusBannerDescriptionDirective,
    StatusBannerTitleDirective,
    TextTip
} from '@nifi/shared';
import { ValidationErrorsTip } from '../tooltips/validation-errors-tip/validation-errors-tip.component';
import { DrainDialogState } from './drain-process-group-dialog.types';
import { getDrainPercentCompleted, getDrainRemainingText } from '../drain/drain-summary.utils';

function isDrained(drainRequest: ProcessGroupDrainRequest): boolean {
    return drainRequest.currentCount === 0;
}

@Component({
    selector: 'drain-process-group-dialog',
    imports: [
        MatDialogModule,
        MatButton,
        AsyncPipe,
        MatProgressBar,
        ErrorBanner,
        NifiTooltipDirective,
        StatusBanner,
        StatusBannerTitleDirective,
        StatusBannerDescriptionDirective
    ],
    templateUrl: './drain-process-group-dialog.html',
    styleUrl: './drain-process-group-dialog.scss'
})
export class DrainProcessGroupDialog {
    private nifiCommon = inject(NiFiCommon);

    @Input() drainState$: Observable<DrainDialogState | null> = of(null);
    @Output() cancelDrain: EventEmitter<void> = new EventEmitter<void>();
    @Output() goToComponent: EventEmitter<DrainBlockingComponent> = new EventEmitter<DrainBlockingComponent>();

    protected readonly ValidationErrorsTip = ValidationErrorsTip;
    protected readonly TextTip = TextTip;

    getStatusMessage(drainState: DrainDialogState): string {
        if (drainState.status === 'ended') {
            if (!drainState.drainRequest) {
                return 'The drain is no longer available.';
            }
            if (drainState.drainRequest.state === 'FAILED') {
                return 'The drain failed.';
            }
            const remainingCount = this.nifiCommon.formatInteger(drainState.drainRequest.currentCount);
            if (!drainState.drainRequest.prepared) {
                return `Drain ended before the source components stopped. ${remainingCount} FlowFiles remain queued.`;
            }
            if (isDrained(drainState.drainRequest)) {
                return 'Drain complete. Source components remain stopped.';
            }
            return `Drain ended. ${remainingCount} FlowFiles remain queued.`;
        }
        if (drainState.status === 'ending') {
            return 'Ending drain...';
        }
        if (drainState.drainRequest && !drainState.drainRequest.prepared) {
            return 'Waiting for the source components to stop...';
        }
        if (drainState.drainRequest && isDrained(drainState.drainRequest)) {
            return 'No FlowFiles remain. Finishing drain...';
        }
        if (drainState.drainRequest) {
            const blockingCount = drainState.drainRequest.blockingComponents.length;
            if (blockingCount === 1) {
                return 'Blocked by 1 component';
            }
            if (blockingCount > 1) {
                return `Blocked by ${this.nifiCommon.formatInteger(blockingCount)} components`;
            }
            return 'Processing queued FlowFiles...';
        }
        return 'Loading drain status...';
    }

    getPercentCompleted(drainRequest: ProcessGroupDrainRequest): number {
        return getDrainPercentCompleted(drainRequest.originalCount, drainRequest.currentCount);
    }

    getRemainingText(drainRequest: ProcessGroupDrainRequest): string {
        return getDrainRemainingText(drainRequest, this.nifiCommon);
    }

    getFailureMessages(failureReason?: string): string[] {
        if (!failureReason) {
            return [];
        }
        return failureReason
            .split('\n')
            .map((line) => line.trim())
            .filter((line) => line.length > 0);
    }

    getBlockingBannerTitle(drainState: DrainDialogState): string {
        return drainState.status === 'ended' ? 'Drain was blocked' : 'Drain is blocked';
    }

    getBlockingComponentLabel(component: DrainBlockingComponent): string {
        return component.name ?? component.id;
    }

    getBlockingComponentDetail(component: DrainBlockingComponent): string {
        const queued = this.formatQueuedCount(component.queuedCount);
        if (component.reason === 'DISABLED') {
            return ` is disabled and has ${queued} queued`;
        }
        return ` is invalid and has ${queued} queued`;
    }

    private formatQueuedCount(count: number): string {
        const formatted = this.nifiCommon.formatInteger(count);
        return count === 1 ? `${formatted} FlowFile` : `${formatted} FlowFiles`;
    }

    getBlockingTooltipComponent(component: DrainBlockingComponent): Type<unknown> | null {
        if (!component.name) {
            return TextTip;
        }
        if (this.getBlockingValidationErrors(component).length > 0) {
            return ValidationErrorsTip;
        }
        return null;
    }

    getBlockingTooltipData(component: DrainBlockingComponent): unknown {
        if (!component.name) {
            return "Not authorized to view this component's details.";
        }
        return {
            isValidating: false,
            validationErrors: this.getBlockingValidationErrors(component)
        };
    }

    private getBlockingValidationErrors(component: DrainBlockingComponent): string[] {
        return component.validationErrors ?? [];
    }

    isProgressIndeterminate(drainRequest: ProcessGroupDrainRequest): boolean {
        return !drainRequest.prepared;
    }
}
