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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.ProcessorStatus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProcessorStatusDescriptorTest {

    private static final long ZERO = 0;

    private static final long FIVE_HUNDRED_NANOSECONDS = 500;

    private static final long METRIC_MILLISECONDS = 10;

    private static final long MINIMUM_METRIC_PERCENTAGE_EXPECTED = 1000;

    private static final ProcessorStatusDescriptor PERCENTAGE_DESCRIPTOR = ProcessorStatusDescriptor.CPU_PERCENTAGE;

    private static final ProcessorStatusDescriptor METRIC_DESCRIPTOR = ProcessorStatusDescriptor.CPU_MILLIS;

    @Test
    void testPercentageValueReducerTaskNanosZero() {
        final MetricDescriptor<ProcessorStatus> descriptor = PERCENTAGE_DESCRIPTOR.getDescriptor();
        final ValueReducer<StatusSnapshot, Long> valueReducer = descriptor.getValueReducer();

        final StatusSnapshot statusSnapshot = getStatusSnapshot(ZERO);
        final List<StatusSnapshot> snapshots = List.of(statusSnapshot);

        final Long reduced = valueReducer.reduce(snapshots);

        assertEquals(ZERO, reduced);
    }

    @Test
    void testPercentageValueReducerTaskNanosFiveHundred() {
        final MetricDescriptor<ProcessorStatus> descriptor = PERCENTAGE_DESCRIPTOR.getDescriptor();
        final ValueReducer<StatusSnapshot, Long> valueReducer = descriptor.getValueReducer();

        final StatusSnapshot statusSnapshot = getStatusSnapshot(FIVE_HUNDRED_NANOSECONDS);
        final List<StatusSnapshot> snapshots = List.of(statusSnapshot);

        final Long reduced = valueReducer.reduce(snapshots);

        assertEquals(MINIMUM_METRIC_PERCENTAGE_EXPECTED, reduced);
    }

    private StatusSnapshot getStatusSnapshot(final long taskNanoseconds) {
        final StatusSnapshot statusSnapshot = mock(StatusSnapshot.class);
        when(statusSnapshot.getStatusMetric(eq(METRIC_DESCRIPTOR.getDescriptor()))).thenReturn(METRIC_MILLISECONDS);
        when(statusSnapshot.getStatusMetric(eq(ProcessorStatusDescriptor.TASK_NANOS.getDescriptor()))).thenReturn(taskNanoseconds);
        return statusSnapshot;
    }
}
