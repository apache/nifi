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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProcessGroupStatusTable } from './process-group-status-table.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { NiFiCommon } from '@nifi/shared';
import { ProcessGroupStatusSnapshotEntity, ProcessGroupStatusSnapshot } from '../../../state';

describe('ProcessGroupStatusTable', () => {
    let component: ProcessGroupStatusTable;
    let fixture: ComponentFixture<ProcessGroupStatusTable>;
    let nifiCommon: NiFiCommon;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ProcessGroupStatusTable, NoopAnimationsModule]
        });
        fixture = TestBed.createComponent(ProcessGroupStatusTable);
        component = fixture.componentInstance;
        nifiCommon = TestBed.inject(NiFiCommon);
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('formatTasks', () => {
        beforeEach(() => {
            // Setup root process group for percentage calculation
            component.rootProcessGroup = {
                processingNanos: 10000000000 // 10 seconds in nanos
            } as ProcessGroupStatusSnapshot;
        });

        it('should convert nanoseconds to milliseconds before formatting', () => {
            const mockEntity: ProcessGroupStatusSnapshotEntity = {
                processGroupStatusSnapshot: {
                    processingNanos: 5000000000 // 5 seconds in nanos = 5000 millis
                } as ProcessGroupStatusSnapshot
            } as ProcessGroupStatusSnapshotEntity;

            const formatDurationSpy = jest.spyOn(nifiCommon, 'formatDuration');

            component.formatTasks(mockEntity);

            // Verify formatDuration was called with milliseconds using NANOS_PER_MILLI constant
            const expectedMillis = 5000000000 / NiFiCommon.NANOS_PER_MILLI;
            expect(formatDurationSpy).toHaveBeenCalledWith(expectedMillis);
        });

        it('should return formatted duration with percentage', () => {
            const mockEntity: ProcessGroupStatusSnapshotEntity = {
                processGroupStatusSnapshot: {
                    processingNanos: 5000000000 // 5 seconds in nanos (50% of root)
                } as ProcessGroupStatusSnapshot
            } as ProcessGroupStatusSnapshotEntity;

            jest.spyOn(nifiCommon, 'formatDuration').mockReturnValue('00:00:05.000');

            const result = component.formatTasks(mockEntity);

            expect(result).toBe('00:00:05.000 (50%)');
        });

        it('should handle zero nanoseconds correctly', () => {
            const mockEntity: ProcessGroupStatusSnapshotEntity = {
                processGroupStatusSnapshot: {
                    processingNanos: 0
                } as ProcessGroupStatusSnapshot
            } as ProcessGroupStatusSnapshotEntity;

            const formatDurationSpy = jest.spyOn(nifiCommon, 'formatDuration');

            component.formatTasks(mockEntity);

            expect(formatDurationSpy).toHaveBeenCalledWith(0);
        });

        it('should handle very large nanosecond values', () => {
            const mockEntity: ProcessGroupStatusSnapshotEntity = {
                processGroupStatusSnapshot: {
                    processingNanos: 3600000000000 // 1 hour in nanos = 3600000 millis
                } as ProcessGroupStatusSnapshot
            } as ProcessGroupStatusSnapshotEntity;

            const formatDurationSpy = jest.spyOn(nifiCommon, 'formatDuration');

            component.formatTasks(mockEntity);

            // Verify conversion to milliseconds using NANOS_PER_MILLI constant
            const expectedMillis = 3600000000000 / NiFiCommon.NANOS_PER_MILLI;
            expect(formatDurationSpy).toHaveBeenCalledWith(expectedMillis);
        });

        it('should calculate correct percentage relative to root process group', () => {
            const mockEntity: ProcessGroupStatusSnapshotEntity = {
                processGroupStatusSnapshot: {
                    processingNanos: 2500000000 // 2.5 seconds in nanos (25% of root)
                } as ProcessGroupStatusSnapshot
            } as ProcessGroupStatusSnapshotEntity;

            jest.spyOn(nifiCommon, 'formatDuration').mockReturnValue('00:00:02.500');

            const result = component.formatTasks(mockEntity);

            expect(result).toBe('00:00:02.500 (25%)');
        });
    });
});
