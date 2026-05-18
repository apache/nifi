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

import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { HttpErrorResponse } from '@angular/common/http';
import { Observable, of, throwError } from 'rxjs';

import { BacklogRow, ProcessorBacklogDialog } from './backlog-dialog.component';
import { Backlog, BacklogRequest, BacklogRequestEntity } from '../../../../../../../state/shared';
import { ProcessorBacklogDialogRequest } from '../../../../../state/flow';
import { FlowService } from '../../../../../service/flow.service';
import { ErrorHelper } from '../../../../../../../service/error-helper.service';

const PROCESSOR_ID = 'a1b2c3d4-0000-0000-0000-000000000000';
const REQUEST_ID = 'e5f6a7b8-0000-0000-0000-000000000000';

function completedRequest(backlog: Backlog | null, failureReason?: string): BacklogRequest {
    return {
        requestId: REQUEST_ID,
        uri: `../nifi-api/processors/${PROCESSOR_ID}/backlog-requests/${REQUEST_ID}`,
        componentId: PROCESSOR_ID,
        complete: true,
        percentCompleted: 100,
        failureReason: failureReason ?? null,
        backlog
    };
}

function inProgressRequest(): BacklogRequest {
    return {
        requestId: REQUEST_ID,
        uri: `../nifi-api/processors/${PROCESSOR_ID}/backlog-requests/${REQUEST_ID}`,
        componentId: PROCESSOR_ID,
        complete: false,
        percentCompleted: 0
    };
}

function requestEntity(request: BacklogRequest): BacklogRequestEntity {
    return { request };
}

interface FlowServiceStub {
    pollProcessorBacklogRequest: ReturnType<typeof vi.fn>;
    deleteProcessorBacklogRequest: ReturnType<typeof vi.fn>;
}

interface CreatedDialog {
    component: ProcessorBacklogDialog;
    fixture: ComponentFixture<ProcessorBacklogDialog>;
    flowService: FlowServiceStub;
}

function createDialog(
    dialogRequest: ProcessorBacklogDialogRequest,
    pollResponses: Observable<BacklogRequestEntity>[] = []
): CreatedDialog {
    let pollCallCount = 0;
    const flowService: FlowServiceStub = {
        pollProcessorBacklogRequest: vi.fn(() => {
            const response = pollResponses[Math.min(pollCallCount, pollResponses.length - 1)];
            pollCallCount++;
            return response;
        }),
        deleteProcessorBacklogRequest: vi.fn(() => of({}) as Observable<BacklogRequestEntity>)
    };

    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
        imports: [ProcessorBacklogDialog],
        providers: [
            { provide: MAT_DIALOG_DATA, useValue: dialogRequest },
            { provide: MatDialogRef, useValue: { close: vi.fn(), keydownEvents: () => of() } },
            { provide: FlowService, useValue: flowService },
            { provide: ErrorHelper, useValue: new ErrorHelper() }
        ]
    });
    const fixture: ComponentFixture<ProcessorBacklogDialog> = TestBed.createComponent(ProcessorBacklogDialog);
    fixture.detectChanges();
    return { component: fixture.componentInstance, fixture, flowService };
}

function rowByLabel(rows: BacklogRow[], label: string): BacklogRow {
    const match = rows.find((row) => row.label === label);
    if (match == null) {
        throw new Error(`Expected a row labelled '${label}', found: ${rows.map((row) => row.label).join(', ')}`);
    }
    return match;
}

describe('ProcessorBacklogDialog', () => {
    it('should create', () => {
        const { component } = createDialog({
            processorId: PROCESSOR_ID,
            requestEntity: requestEntity(
                completedRequest({
                    flowFileCount: 1,
                    byteCount: 2,
                    recordCount: 3,
                    precision: 'EXACT',
                    lastCaughtUp: '2026-05-15T12:00:00Z',
                    formattedLastCaughtUp: '5 mins ago'
                })
            )
        });
        expect(component).toBeTruthy();
    });

    describe('row building', () => {
        it('uses server-provided formatted values and exposes raw values as tooltips', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(
                    completedRequest({
                        flowFileCount: 1025,
                        formattedFlowFileCount: '1,025',
                        byteCount: 5247340800,
                        formattedByteCount: '4.89 GB',
                        recordCount: 2050,
                        formattedRecordCount: '2,050',
                        precision: 'EXACT',
                        lastCaughtUp: '2026-05-15T12:00:00Z',
                        formattedLastCaughtUp: '5 mins ago'
                    })
                )
            });

            const flowFiles: BacklogRow = rowByLabel(component.rows, 'FlowFiles');
            expect(flowFiles.value).toBe('1,025');
            expect(flowFiles.tooltip).toBe('1025');

            const bytes: BacklogRow = rowByLabel(component.rows, 'Bytes');
            expect(bytes.value).toBe('4.89 GB');
            expect(bytes.tooltip).toBe('5247340800 bytes');

            const records: BacklogRow = rowByLabel(component.rows, 'Records');
            expect(records.value).toBe('2,050');
            expect(records.tooltip).toBe('2050');

            const precision: BacklogRow = rowByLabel(component.rows, 'Precision');
            expect(precision.value).toBe('EXACT');
            expect(precision.tooltip).toBeUndefined();

            const lastCaughtUp: BacklogRow = rowByLabel(component.rows, 'Last caught up');
            // The absolute timestamp is rendered in the test runner's local timezone, which varies by
            // machine, so assert the shape — "yyyy-MM-dd HH:mm:ss <ZONE> (5 mins ago)" — instead of an
            // exact string. The relative-time portion must be the server-provided string verbatim, and
            // the raw ISO timestamp must be preserved as the tooltip.
            expect(lastCaughtUp.value).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ \(5 mins ago\)$/);
            expect(lastCaughtUp.tooltip).toBe('2026-05-15T12:00:00Z');
        });

        it('falls back to the raw numeric value when no formatted value is provided', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(
                    completedRequest({
                        flowFileCount: 7,
                        byteCount: 42,
                        precision: 'AT_LEAST'
                    })
                )
            });

            expect(rowByLabel(component.rows, 'FlowFiles').value).toBe('7');
            expect(rowByLabel(component.rows, 'Bytes').value).toBe('42 B');
        });

        it('marks dimensions the backlog did not report as unset so the template renders "No value set"', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(completedRequest({ precision: 'EXACT' }))
            });

            expect(rowByLabel(component.rows, 'FlowFiles').value).toBeNull();
            expect(rowByLabel(component.rows, 'FlowFiles').tooltip).toBeUndefined();
            expect(rowByLabel(component.rows, 'Bytes').value).toBeNull();
            expect(rowByLabel(component.rows, 'Records').value).toBeNull();
            expect(rowByLabel(component.rows, 'Last caught up').value).toBeNull();
        });

        it('marks every measurement as unset when the backlog is null', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(completedRequest(null))
            });

            for (const row of component.rows) {
                expect(row.value).toBeNull();
                expect(row.tooltip).toBeUndefined();
            }
        });
    });

    describe('Last caught up row formatting', () => {
        it('renders the absolute timestamp paired with the server-provided "(now)" string', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(
                    completedRequest({
                        flowFileCount: 0,
                        byteCount: 0,
                        recordCount: 0,
                        precision: 'EXACT',
                        lastCaughtUp: '2026-05-15T12:00:00Z',
                        formattedLastCaughtUp: 'now'
                    })
                )
            });
            expect(rowByLabel(component.rows, 'Last caught up').value).toMatch(
                /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ \(now\)$/
            );
        });

        it('renders any server-provided relative-time string verbatim inside parentheses', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(
                    completedRequest({
                        flowFileCount: 1,
                        precision: 'AT_LEAST',
                        lastCaughtUp: '2026-05-14T12:00:00Z',
                        formattedLastCaughtUp: 'yesterday'
                    })
                )
            });
            expect(rowByLabel(component.rows, 'Last caught up').value).toMatch(/^.+ \(yesterday\)$/);
        });

        it('renders just the absolute timestamp when the server omits the relative-time string', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(
                    completedRequest({
                        flowFileCount: 1,
                        precision: 'AT_LEAST',
                        lastCaughtUp: '2026-05-15T12:00:00Z'
                    })
                )
            });
            const value = rowByLabel(component.rows, 'Last caught up').value;
            expect(value).not.toBeNull();
            expect(value).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+$/);
        });

        it('renders "No value set" when the timestamp is unparseable', () => {
            const { component } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(
                    completedRequest({
                        lastCaughtUp: 'not a real timestamp',
                        formattedLastCaughtUp: '5 mins ago'
                    })
                )
            });
            expect(rowByLabel(component.rows, 'Last caught up').value).toBeNull();
        });
    });

    describe('asynchronous request lifecycle', () => {
        it('shows the initial error message without polling when the request could not be submitted', () => {
            const { component, flowService } = createDialog({
                processorId: PROCESSOR_ID,
                errorMessage: 'Not authorized to view this backlog.'
            });

            expect(component.complete).toBe(true);
            expect(component.errorMessage).toBe('Not authorized to view this backlog.');
            expect(flowService.pollProcessorBacklogRequest).not.toHaveBeenCalled();
        });

        it('polls until the request completes, populates the rows, and deletes the request', fakeAsync(() => {
            const { component, fixture, flowService } = createDialog(
                {
                    processorId: PROCESSOR_ID,
                    requestEntity: requestEntity(inProgressRequest())
                },
                [of(requestEntity(inProgressRequest())), of(requestEntity(completedRequest({ flowFileCount: 5 })))]
            );

            expect(component.complete).toBe(false);

            tick(2000);
            fixture.detectChanges(false);
            expect(component.complete).toBe(false);
            expect(flowService.deleteProcessorBacklogRequest).not.toHaveBeenCalled();

            tick(2000);
            fixture.detectChanges(false);

            expect(component.complete).toBe(true);
            expect(rowByLabel(component.rows, 'FlowFiles').value).toBe('5');
            expect(flowService.deleteProcessorBacklogRequest).toHaveBeenCalledTimes(1);
            expect(flowService.deleteProcessorBacklogRequest).toHaveBeenCalledWith(PROCESSOR_ID, REQUEST_ID);

            fixture.destroy();
        }));

        it('surfaces the failure reason once the request completes unsuccessfully', fakeAsync(() => {
            const { component, fixture } = createDialog(
                {
                    processorId: PROCESSOR_ID,
                    requestEntity: requestEntity(inProgressRequest())
                },
                [of(requestEntity(completedRequest(null, 'Interrupted while determining Kafka backlog')))]
            );

            tick(2000);
            fixture.detectChanges(false);

            expect(component.complete).toBe(true);
            expect(component.errorMessage).toBe('Interrupted while determining Kafka backlog');
            expect(component.rows).toEqual([]);

            fixture.destroy();
        }));

        it('stops polling and surfaces an error when a poll request itself fails', fakeAsync(() => {
            const { component, fixture, flowService } = createDialog(
                {
                    processorId: PROCESSOR_ID,
                    requestEntity: requestEntity(inProgressRequest())
                },
                [throwError(() => new HttpErrorResponse({ status: 500, error: 'Server error' }))]
            );

            tick(2000);
            fixture.detectChanges(false);

            expect(component.complete).toBe(true);
            expect(component.errorMessage).toContain('Server error');

            const callsBeforeFurtherTicks = flowService.pollProcessorBacklogRequest.mock.calls.length;
            tick(4000);
            expect(flowService.pollProcessorBacklogRequest.mock.calls.length).toBe(callsBeforeFurtherTicks);

            fixture.destroy();
        }));

        it('deletes the in-progress request immediately when the dialog is cancelled', () => {
            const { component, fixture, flowService } = createDialog({
                processorId: PROCESSOR_ID,
                requestEntity: requestEntity(inProgressRequest())
            });

            component.cancel();
            fixture.destroy();

            expect(flowService.deleteProcessorBacklogRequest).toHaveBeenCalledTimes(1);
            expect(flowService.deleteProcessorBacklogRequest).toHaveBeenCalledWith(PROCESSOR_ID, REQUEST_ID);
        });

        it('does not delete an already-deleted request a second time when the dialog is destroyed', fakeAsync(() => {
            const { fixture, flowService } = createDialog(
                {
                    processorId: PROCESSOR_ID,
                    requestEntity: requestEntity(inProgressRequest())
                },
                [of(requestEntity(completedRequest({ flowFileCount: 1 })))]
            );

            tick(2000);
            fixture.detectChanges(false);
            expect(flowService.deleteProcessorBacklogRequest).toHaveBeenCalledTimes(1);

            fixture.destroy();
            expect(flowService.deleteProcessorBacklogRequest).toHaveBeenCalledTimes(1);
        }));
    });
});
