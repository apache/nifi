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

import { Component, DestroyRef, inject, OnDestroy } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { MatTableModule } from '@angular/material/table';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { Backlog, BacklogRequest } from '../../../../../../../state/shared';
import { ProcessorBacklogDialogRequest } from '../../../../../state/flow';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { FlowService } from '../../../../../service/flow.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../../../../service/error-helper.service';
import { asyncScheduler, catchError, interval, of, Subscription, switchMap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

/**
 * How often to poll the backlog request while it remains incomplete.
 */
const POLL_INTERVAL_MILLIS = 2000;

/**
 * Row in the vertical two-column backlog table.
 * - {@code label}: shown in the "Measurement" column.
 * - {@code value}: formatted display string. {@code null} signals that the backlog response did not
 *   carry a value for this row and the cell should render the standard "No value set" placeholder
 *   used throughout the NiFi UI for unset property values.
 * - {@code tooltip}: optional raw underlying value (for example, the raw byte count or the raw ISO
 *   timestamp) revealed when the user hovers over the value cell. Omitted when no underlying raw
 *   form exists separate from the displayed value, or when the value itself is unset.
 */
export interface BacklogRow {
    label: string;
    value: string | null;
    tooltip?: string;
}

/**
 * Displays the outcome of an asynchronous backlog request for a single processor. Determining a
 * backlog may require contacting an external system and can take a while to complete, so the
 * request is created on the server and polled here until it either completes or fails. Closing
 * the dialog by any means (the Cancel button, the OK button, the Escape key, or the backdrop)
 * deletes the request on the server, which cancels the background determination if it has not
 * yet completed.
 */
@Component({
    selector: 'processor-backlog-dialog',
    imports: [MatButton, MatDialogModule, MatTableModule, MatTooltipModule, MatProgressBarModule],
    templateUrl: './backlog-dialog.component.html',
    styleUrls: ['./backlog-dialog.component.scss']
})
export class ProcessorBacklogDialog extends CloseOnEscapeDialog implements OnDestroy {
    private dialogRequest = inject<ProcessorBacklogDialogRequest>(MAT_DIALOG_DATA);
    private backlogDialogRef = inject<MatDialogRef<ProcessorBacklogDialog>>(MatDialogRef);
    private flowService = inject(FlowService);
    private errorHelper = inject(ErrorHelper);
    private destroyRef = inject(DestroyRef);

    private readonly processorId: string;
    private requestId: string | null = null;
    private requestDeleted = false;
    private pollingSubscription: Subscription | null = null;

    readonly displayedColumns: string[] = ['measurement', 'value'];
    rows: BacklogRow[] = [];
    errorMessage?: string;
    complete = false;

    constructor() {
        super();
        this.processorId = this.dialogRequest.processorId;

        if (this.dialogRequest.errorMessage) {
            this.errorMessage = this.dialogRequest.errorMessage;
            this.complete = true;
        } else if (this.dialogRequest.requestEntity) {
            this.applyRequest(this.dialogRequest.requestEntity.request);
            if (!this.complete) {
                this.startPolling();
            }
        }
    }

    ngOnDestroy(): void {
        this.deleteRequestIfNecessary();
    }

    cancel(): void {
        this.backlogDialogRef.close();
    }

    private startPolling(): void {
        this.pollingSubscription = interval(POLL_INTERVAL_MILLIS, asyncScheduler)
            .pipe(
                switchMap(() =>
                    this.flowService.pollProcessorBacklogRequest(this.processorId, this.requestId as string)
                ),
                catchError((errorResponse: HttpErrorResponse) => {
                    this.stopPolling();
                    this.errorMessage = this.errorHelper.getErrorString(errorResponse);
                    this.complete = true;
                    return of(null);
                }),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((entity) => {
                if (entity) {
                    this.applyRequest(entity.request);
                    if (this.complete) {
                        this.stopPolling();
                        this.deleteRequestIfNecessary();
                    }
                }
            });
    }

    private stopPolling(): void {
        this.pollingSubscription?.unsubscribe();
        this.pollingSubscription = null;
    }

    private applyRequest(request: BacklogRequest): void {
        this.requestId = request.requestId;
        this.complete = request.complete;
        if (request.complete) {
            if (request.failureReason) {
                this.errorMessage = request.failureReason;
            } else {
                this.rows = this.buildRows(request.backlog);
            }
        }
    }

    private deleteRequestIfNecessary(): void {
        this.stopPolling();
        if (this.requestId && !this.requestDeleted) {
            this.requestDeleted = true;
            this.flowService.deleteProcessorBacklogRequest(this.processorId, this.requestId).subscribe();
        }
    }

    private buildRows(backlog: Backlog | null | undefined): BacklogRow[] {
        return [
            this.buildCountRow('FlowFiles', backlog?.flowFileCount, backlog?.formattedFlowFileCount),
            this.buildDataSizeRow('Bytes', backlog?.byteCount, backlog?.formattedByteCount),
            this.buildCountRow('Records', backlog?.recordCount, backlog?.formattedRecordCount),
            this.buildPrecisionRow(backlog?.precision),
            this.buildLastCaughtUpRow(backlog?.lastCaughtUp, backlog?.formattedLastCaughtUp)
        ];
    }

    private buildCountRow(label: string, rawValue: number | undefined, formattedValue: string | undefined): BacklogRow {
        if (rawValue == null) {
            return { label, value: null };
        }
        const displayed = formattedValue ?? String(rawValue);
        return { label, value: displayed, tooltip: String(rawValue) };
    }

    private buildDataSizeRow(
        label: string,
        rawValue: number | undefined,
        formattedValue: string | undefined
    ): BacklogRow {
        if (rawValue == null) {
            return { label, value: null };
        }
        const displayed = formattedValue ?? `${rawValue} B`;
        return { label, value: displayed, tooltip: `${rawValue} bytes` };
    }

    private buildPrecisionRow(precision: string | undefined): BacklogRow {
        if (precision == null) {
            return { label: 'Precision', value: null };
        }
        return { label: 'Precision', value: precision };
    }

    /**
     * Render the "Last caught up" row as the absolute timestamp in the user's local timezone, paired
     * with the server-computed relative-time string. The server emits {@code formattedLastCaughtUp}
     * alongside the raw {@code lastCaughtUp} so that the relative phrase ("5 mins ago", "yesterday",
     * "now", etc.) is consistent with the values shown for {@code formattedFlowFileCount},
     * {@code formattedByteCount}, and {@code formattedRecordCount} — all computed once on the server
     * rather than recomputed on every UI render. The absolute portion stays on the client because
     * formatting the timestamp in the user's local timezone requires the browser's locale.
     */
    private buildLastCaughtUpRow(
        lastCaughtUp: string | null | undefined,
        formattedLastCaughtUp: string | undefined
    ): BacklogRow {
        if (lastCaughtUp == null) {
            return { label: 'Last caught up', value: null };
        }

        const lastCaughtUpDate = new Date(lastCaughtUp);
        if (Number.isNaN(lastCaughtUpDate.getTime())) {
            return { label: 'Last caught up', value: null };
        }

        const formattedDate = formatLocalTimestampWithZone(lastCaughtUpDate);
        if (formattedLastCaughtUp == null) {
            return {
                label: 'Last caught up',
                value: formattedDate,
                tooltip: lastCaughtUp
            };
        }
        return {
            label: 'Last caught up',
            value: `${formattedDate} (${formattedLastCaughtUp})`,
            tooltip: lastCaughtUp
        };
    }
}

/**
 * Format an absolute {@link Date} in the browser's local time using {@link Intl.DateTimeFormat} with
 * {@code timeZoneName: 'short'}, producing strings such as {@code "2026-05-15 08:45:30 EDT"}. The
 * Angular {@code DatePipe} 'z' symbol is unreliable here because its locale data typically falls back
 * to a {@code "GMT-4"}-style offset when a specific timezone abbreviation is unavailable, whereas the
 * ICU-backed {@link Intl} formatter consistently emits the browser's short timezone name.
 */
export function formatLocalTimestampWithZone(date: Date): string {
    const formatter = new Intl.DateTimeFormat(undefined, {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        hourCycle: 'h23',
        timeZoneName: 'short'
    });
    const parts = formatter.formatToParts(date);
    const partOfType = (type: Intl.DateTimeFormatPartTypes): string =>
        parts.find((part) => part.type === type)?.value ?? '';
    return `${partOfType('year')}-${partOfType('month')}-${partOfType('day')} ${partOfType('hour')}:${partOfType('minute')}:${partOfType('second')} ${partOfType('timeZoneName')}`;
}
