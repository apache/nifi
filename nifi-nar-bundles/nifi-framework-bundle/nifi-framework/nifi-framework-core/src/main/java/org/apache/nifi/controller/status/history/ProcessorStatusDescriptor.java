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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;

public enum ProcessorStatusDescriptor {
    BYTES_READ(new StandardMetricDescriptor<ProcessorStatus>(
        "bytesRead",
        "Bytes Read (5 mins)",
        "The total number of bytes read from the Content Repository by this Processor in the past 5 minutes",
        Formatter.DATA_SIZE,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getBytesRead();
            }
        })),

    BYTES_WRITTEN(new StandardMetricDescriptor<ProcessorStatus>(
        "bytesWritten",
        "Bytes Written (5 mins)",
        "The total number of bytes written to the Content Repository by this Processor in the past 5 minutes",
        Formatter.DATA_SIZE,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getBytesWritten();
            }
        })),

    BYTES_TRANSFERRED(new StandardMetricDescriptor<ProcessorStatus>(
        "bytesTransferred",
        "Bytes Transferred (5 mins)",
        "The total number of bytes read from or written to the Content Repository by this Processor in the past 5 minutes",
        Formatter.DATA_SIZE,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getBytesRead() + status.getBytesWritten();
            }
        })),

    INPUT_BYTES(new StandardMetricDescriptor<ProcessorStatus>(
        "inputBytes",
        "Bytes In (5 mins)",
        "The cumulative size of all FlowFiles that this Processor has pulled from its queues in the past 5 minutes",
        Formatter.DATA_SIZE,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getInputBytes();
            }
        })),

    INPUT_COUNT(new StandardMetricDescriptor<ProcessorStatus>(
        "inputCount",
        "FlowFiles In (5 mins)",
        "The number of FlowFiles that this Processor has pulled from its queues in the past 5 minutes",
        Formatter.COUNT, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getInputCount());
            }
        })),
    OUTPUT_BYTES(new StandardMetricDescriptor<ProcessorStatus>(
        "outputBytes",
        "Bytes Out (5 mins)",
        "The cumulative size of all FlowFiles that this Processor has transferred to downstream queues in the past 5 minutes",
        Formatter.DATA_SIZE,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getOutputBytes();
            }
        })),

    OUTPUT_COUNT(new StandardMetricDescriptor<ProcessorStatus>(
        "outputCount",
        "FlowFiles Out (5 mins)",
        "The number of FlowFiles that this Processor has transferred to downstream queues in the past 5 minutes",
        Formatter.COUNT,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getOutputCount());
            }
        })),

    TASK_COUNT(new StandardMetricDescriptor<ProcessorStatus>(
        "taskCount",
        "Tasks (5 mins)",
        "The number of tasks that this Processor has completed in the past 5 minutes",
        Formatter.COUNT,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getInvocations());
            }
        })),

    TASK_MILLIS(new StandardMetricDescriptor<ProcessorStatus>(
        "taskMillis",
        "Total Task Duration (5 mins)",
        "The total number of thread-milliseconds that the Processor has used to complete its tasks in the past 5 minutes",
        Formatter.DURATION,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return TimeUnit.MILLISECONDS.convert(status.getProcessingNanos(), TimeUnit.NANOSECONDS);
            }
        })),

    FLOWFILES_REMOVED(new StandardMetricDescriptor<ProcessorStatus>(
        "flowFilesRemoved",
        "FlowFiles Removed (5 mins)",
        "The total number of FlowFiles removed by this Processor in the last 5 minutes",
        Formatter.COUNT,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getFlowFilesRemoved());
            }
        })),

    AVERAGE_LINEAGE_DURATION(new StandardMetricDescriptor<ProcessorStatus>(
        "averageLineageDuration",
        "Average Lineage Duration (5 mins)",
        "The average amount of time that a FlowFile took to process (from receipt until this Processor finished processing it) in the past 5 minutes.",
        Formatter.DURATION,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getAverageLineageDuration(TimeUnit.MILLISECONDS);
            }
        }, new ValueReducer<StatusSnapshot, Long>() {
            @Override
            public Long reduce(final List<StatusSnapshot> values) {
                long millis = 0L;
                int count = 0;

                for (final StatusSnapshot snapshot : values) {
                    final long removed = snapshot.getStatusMetrics().get(FLOWFILES_REMOVED.getDescriptor()).longValue();
                    count += removed;

                    count += snapshot.getStatusMetrics().get(OUTPUT_COUNT.getDescriptor()).longValue();

                    final long avgMillis = snapshot.getStatusMetrics().get(AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue();
                    final long totalMillis = avgMillis * removed;
                    millis += totalMillis;
                }

                return count == 0 ? 0 : millis / count;
            }
        }
    )),

    AVERAGE_TASK_MILLIS(new StandardMetricDescriptor<ProcessorStatus>(
        "averageTaskMillis",
        "Average Task Duration",
        "The average duration it took this Processor to complete a task, as averaged over the past 5 minutes",
        Formatter.DURATION,
        new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getInvocations() == 0 ? 0 : TimeUnit.MILLISECONDS.convert(status.getProcessingNanos(), TimeUnit.NANOSECONDS) / status.getInvocations();
            }
        },
        new ValueReducer<StatusSnapshot, Long>() {
            @Override
            public Long reduce(final List<StatusSnapshot> values) {
                long procMillis = 0L;
                int invocations = 0;

                for (final StatusSnapshot snapshot : values) {
                    procMillis += snapshot.getStatusMetrics().get(TASK_MILLIS.getDescriptor()).longValue();
                    invocations += snapshot.getStatusMetrics().get(TASK_COUNT.getDescriptor()).intValue();
                }

                if (invocations == 0) {
                    return 0L;
                }

                return procMillis / invocations;
            }
        }));

    private MetricDescriptor<ProcessorStatus> descriptor;

    private ProcessorStatusDescriptor(final MetricDescriptor<ProcessorStatus> descriptor) {
        this.descriptor = descriptor;
    }

    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<ProcessorStatus> getDescriptor() {
        return descriptor;
    }
}
