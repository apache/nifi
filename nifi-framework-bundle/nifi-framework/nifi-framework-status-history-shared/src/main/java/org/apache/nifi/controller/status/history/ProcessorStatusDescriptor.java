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

import org.apache.nifi.controller.status.ProcessingPerformanceStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public enum ProcessorStatusDescriptor {
    BYTES_READ(
            "bytesRead",
            "Bytes Read (5 mins)",
            "The total number of bytes read from the Content Repository by this Processor in the past 5 minutes",
            Formatter.DATA_SIZE,
            ProcessorStatus::getBytesRead),

    BYTES_WRITTEN(
            "bytesWritten",
            "Bytes Written (5 mins)",
            "The total number of bytes written to the Content Repository by this Processor in the past 5 minutes",
            Formatter.DATA_SIZE,
            ProcessorStatus::getBytesWritten),

    BYTES_TRANSFERRED(
            "bytesTransferred",
            "Bytes Transferred (5 mins)",
            "The total number of bytes read from or written to the Content Repository by this Processor in the past 5 minutes",
            Formatter.DATA_SIZE,
                s -> s.getBytesRead() + s.getBytesWritten()),

    INPUT_BYTES(
            "inputBytes",
            "Bytes In (5 mins)",
            "The cumulative size of all FlowFiles that this Processor has pulled from its queues in the past 5 minutes",
            Formatter.DATA_SIZE,
            ProcessorStatus::getInputBytes),

    INPUT_COUNT(
            "inputCount",
            "FlowFiles In (5 mins)",
            "The number of FlowFiles that this Processor has pulled from its queues in the past 5 minutes",
            Formatter.COUNT,
                s -> (long) s.getInputCount()),

    OUTPUT_BYTES(
            "outputBytes",
            "Bytes Out (5 mins)",
            "The cumulative size of all FlowFiles that this Processor has transferred to downstream queues in the past 5 minutes",
            Formatter.DATA_SIZE,
            ProcessorStatus::getOutputBytes),

    OUTPUT_COUNT(
            "outputCount",
            "FlowFiles Out (5 mins)",
            "The number of FlowFiles that this Processor has transferred to downstream queues in the past 5 minutes",
            Formatter.COUNT,
                s -> (long) s.getOutputCount()),

    TASK_COUNT(
            "taskCount",
            "Tasks (5 mins)",
            "The number of tasks that this Processor has completed in the past 5 minutes",
            Formatter.COUNT,
                s -> (long) s.getInvocations()),

    TASK_MILLIS(
            "taskMillis",
            "Total Task Duration (5 mins)",
            "The total number of thread-milliseconds that the Processor has used to complete its tasks in the past 5 minutes",
            Formatter.DURATION,
                s -> TimeUnit.MILLISECONDS.convert(s.getProcessingNanos(), TimeUnit.NANOSECONDS)),

    TASK_NANOS(
            "taskNanos",
            "Total Task Time (nanos)",
            "The total number of thread-nanoseconds that the Processor has used to complete its tasks in the past 5 minutes",
            Formatter.COUNT,
            ProcessorStatus::getProcessingNanos,
            false),

    FLOWFILES_REMOVED(
            "flowFilesRemoved",
            "FlowFiles Removed (5 mins)",
            "The total number of FlowFiles removed by this Processor in the last 5 minutes",
            Formatter.COUNT,
                s -> (long) s.getFlowFilesRemoved()),

    AVERAGE_LINEAGE_DURATION(
            "averageLineageDuration",
            "Average Lineage Duration (5 mins)",
            "The average amount of time that a FlowFile took to process (from receipt until this Processor finished processing it) in the past 5 minutes.",
            Formatter.DURATION,
                s -> s.getAverageLineageDuration(TimeUnit.MILLISECONDS),
            new ValueReducer<>() {
        @Override
        public Long reduce(final List<StatusSnapshot> values) {
            long millis = 0L;
            long count = 0;

            for (final StatusSnapshot snapshot : values) {
                final long removed = snapshot.getStatusMetric(FLOWFILES_REMOVED.getDescriptor());
                final long outputCount = snapshot.getStatusMetric(OUTPUT_COUNT.getDescriptor());
                final long processed = removed + outputCount;

                count += processed;

                final long avgMillis = snapshot.getStatusMetric(AVERAGE_LINEAGE_DURATION.getDescriptor());
                final long totalMillis = avgMillis * processed;
                millis += totalMillis;
            }

            return count == 0 ? 0 : millis / count;
        }
    },
            true
    ),

    AVERAGE_TASK_NANOS(
            "averageTaskNanos",
            "Average Task Duration (nanoseconds)",
            "The average number of nanoseconds it took this Processor to complete a task, over the past 5 minutes",
            Formatter.COUNT,
                s -> s.getInvocations() == 0 ? 0 : s.getProcessingNanos() / s.getInvocations(),
            new ValueReducer<>() {
        @Override
        public Long reduce(final List<StatusSnapshot> values) {
            long procNanos = 0L;
            int invocations = 0;

            for (final StatusSnapshot snapshot : values) {
                final Long taskNanos = snapshot.getStatusMetric(TASK_NANOS.getDescriptor());
                if (taskNanos != null) {
                    procNanos += taskNanos;
                }

                final Long taskInvocations = snapshot.getStatusMetric(TASK_COUNT.getDescriptor());
                if (taskInvocations != null) {
                    invocations += taskInvocations.intValue();
                }
            }

            if (invocations == 0) {
                return 0L;
            }

            return procNanos / invocations;
        }
    },
            true
    ),

    CPU_MILLIS(
            "cpuTime",
            "CPU Time (5 mins)",
            "The total amount of time that the Processor has used the CPU in the past 5 minutes. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.DURATION,
                status -> nanosToMillis(status, ProcessingPerformanceStatus::getCpuDuration)
    ),

    CPU_PERCENTAGE(
            "cpuPercentage",
            "CPU Percentage (5 mins)",
            "Of the time that the Processor was running in the past 5 minutes, the percentage of that time that the Processor was using the CPU. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.COUNT,
                status -> nanosValue(status, ProcessingPerformanceStatus::getCpuDuration) * 100 / Math.max(1, status.getProcessingNanos()),
            processingPercentage(CPU_MILLIS.getDescriptor()),
            true
    ),

    CONTENT_REPO_READ_MILLIS(
            "contentRepoReadTime",
            "Content Repo Read Time (5 mins)",
            "The amount of time that the Processor has spent reading from the Content Repository in the past 5 minutes. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.DURATION,
                status -> nanosToMillis(status, ProcessingPerformanceStatus::getContentReadDuration)
    ),

    CONTENT_REPO_READ_PERCENTAGE(
            "contentRepoReadPercentage",
            "Content Repo Read Percentage (5 mins)",
            "Of the time that the Processor was running in the past 5 minutes, the percentage of that time that the Processor was reading from the Content Repository. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.COUNT,
                status -> nanosValue(status, ProcessingPerformanceStatus::getContentReadDuration) * 100 / Math.max(1, status.getProcessingNanos()),
            processingPercentage(CONTENT_REPO_READ_MILLIS.getDescriptor()),
            true
    ),

    CONTENT_REPO_WRITE_MILLIS(
            "contentRepoWriteTime",
            "Content Repo Write Time (5 mins)",
            "The total amount of time that the Processor has spent writing to the Content Repository in the past 5 minutes. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.DURATION,
                status -> nanosToMillis(status, ProcessingPerformanceStatus::getContentWriteDuration)
    ),

    CONTENT_REPO_WRITE_PERCENTAGE(
            "contentRepoWritePercentage",
            "Content Repo Write Percentage (5 mins)",
            "Of the time that the Processor was running in the past 5 minutes, the percentage of that time that the Processor was writing to the Content Repository. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.COUNT,
                status -> nanosValue(status, ProcessingPerformanceStatus::getContentWriteDuration) * 100 / Math.max(1, status.getProcessingNanos()),
            processingPercentage(CONTENT_REPO_WRITE_MILLIS.getDescriptor()),
            true
    ),

    SESSION_COMMIT_MILLIS(
            "sessionCommitTime",
            "Session Commit Time (5 mins)",
            "The total amount of time that the Processor has spent waiting for the framework to commit its ProcessSession in the past 5 minutes. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.DURATION,
                status -> nanosToMillis(status, ProcessingPerformanceStatus::getSessionCommitDuration)
    ),

    SESSION_COMMIT_PERCENTAGE(
            "sessionCommitPercentage",
            "Session Commit Percentage (5 mins)",
            "Of the time that the Processor was running in the past 5 minutes, the percentage of that time that the Processor was waiting for hte framework to commit its ProcessSession. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.COUNT,
                status -> nanosValue(status, ProcessingPerformanceStatus::getSessionCommitDuration) * 100 / Math.max(1, status.getProcessingNanos()),
            processingPercentage(SESSION_COMMIT_MILLIS.getDescriptor()),
            true
    ),

    GARBAGE_COLLECTION_MILLIS(
            "garbageCollectionTime",
            "Garbage Collection Time (5 mins)",
            "The total amount of time that the Processor has spent blocked on Garbage Collection in the past 5 minutes. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.DURATION,
                status -> {
                    final ProcessingPerformanceStatus perfStatus = status.getProcessingPerformanceStatus();
                    if (perfStatus == null) {
                        return 0L;
                    }
                    // Garbage Collection is reported in milliseconds rather than nanos.
                    return perfStatus.getGarbageCollectionDuration();
                }
    ),

    GARBAGE_COLLECTION_PERCENTAGE(
            "garbageCollectionPercentage",
            "Garbage Collection Percentage (5 mins)",
            "Of the time that the Processor was running in the past 5 minutes, the percentage of that time that the Processor spent blocked on Garbage Collection. " +
            "Note, this metric may be unavailable, depending on configuration of the `nifi.performance.tracking.percentage` property.",
            Formatter.COUNT,
                status -> {
                    final ProcessingPerformanceStatus perfStatus = status.getProcessingPerformanceStatus();
                    if (perfStatus == null) {
                        return 0L;
                    }
                    // Garbage Collection is reported in milliseconds rather than nanos.
                    final long percentage = perfStatus.getGarbageCollectionDuration() * 100 / Math.max(1, status.getProcessingNanos());
                    if (percentage == 0 && perfStatus.getGarbageCollectionDuration() > 0) {
                        // If the value is non-zero but less than 1%, we want to return 1%.
                        return 1L;
                    }
                    return percentage;
                },
            processingPercentage(GARBAGE_COLLECTION_MILLIS.getDescriptor()),
            true
    );

    private static long nanosToMillis(final ProcessorStatus procStatus, final Function<ProcessingPerformanceStatus, Long> metricTransform) {
        final ProcessingPerformanceStatus perfStatus = procStatus.getProcessingPerformanceStatus();
        if (perfStatus == null) {
            return 0;
        }

        final long nanos = metricTransform.apply(perfStatus);
        final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
        if (millis == 0 && nanos > 0) {
            // If the value is non-zero but less than 1ms, we want to return 1ms.
            return 1;
        }

        return millis;
    }

    private static ValueReducer<StatusSnapshot, Long> processingPercentage(final MetricDescriptor<?> metricDescriptor) {
        return new ValueReducer<>() {
            @Override
            public Long reduce(final List<StatusSnapshot> values) {
                long procNanos = 0L;
                long metricMillis = 0L;

                for (final StatusSnapshot snapshot : values) {
                    final long millis = snapshot.getStatusMetric(metricDescriptor);
                    metricMillis += millis;

                    final long taskNanos = snapshot.getStatusMetric(ProcessorStatusDescriptor.TASK_NANOS.getDescriptor());
                    procNanos += taskNanos;
                }

                if (procNanos == 0) {
                    return 0L;
                }

                final long procMillis = TimeUnit.MILLISECONDS.convert(procNanos, TimeUnit.NANOSECONDS);
                // Convert processing milliseconds to 1 if nanoseconds is 0 to avoid ArithmeticException
                final long convertedProcessingMilliseconds = procMillis == 0 ? 1 : procMillis;

                return metricMillis * 100 / convertedProcessingMilliseconds;
            }
        };
    }

    private static long nanosValue(final ProcessorStatus procStatus, final Function<ProcessingPerformanceStatus, Long> metricTransform) {
        final ProcessingPerformanceStatus perfStatus = procStatus.getProcessingPerformanceStatus();
        if (perfStatus == null) {
            return 0;
        }

        return metricTransform.apply(perfStatus);
    }


    private final MetricDescriptor<ProcessorStatus> descriptor;
    private final boolean visible;

    ProcessorStatusDescriptor(final String field, final String label, final String description,
                              final MetricDescriptor.Formatter formatter, final ValueMapper<ProcessorStatus> valueFunction) {

        this(field, label, description, formatter, valueFunction, true);
    }

    ProcessorStatusDescriptor(final String field, final String label, final String description,
                              final MetricDescriptor.Formatter formatter, final ValueMapper<ProcessorStatus> valueFunction, final boolean visible) {

        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction);
        this.visible = visible;
    }

    ProcessorStatusDescriptor(final String field, final String label, final String description,
                              final MetricDescriptor.Formatter formatter, final ValueMapper<ProcessorStatus> valueFunction,
                              final ValueReducer<StatusSnapshot, Long> reducer, final boolean visible) {

        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction, reducer);
        this.visible = visible;
    }



    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<ProcessorStatus> getDescriptor() {
        return descriptor;
    }

    public boolean isVisible() {
        return visible;
    }
}
