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

import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;

public enum ProcessGroupStatusDescriptor {

    BYTES_READ(new StandardMetricDescriptor<ProcessGroupStatus>(
        "bytesRead",
        "Bytes Read (5 mins)",
        "The total number of bytes read from Content Repository by Processors in this Process Group in the past 5 minutes",
        Formatter.DATA_SIZE,
        s -> s.getBytesRead())),

    BYTES_WRITTEN(new StandardMetricDescriptor<ProcessGroupStatus>("bytesWritten",
        "Bytes Written (5 mins)",
        "The total number of bytes written to Content Repository by Processors in this Process Group in the past 5 minutes",
        Formatter.DATA_SIZE,
        s -> s.getBytesWritten())),

    BYTES_TRANSFERRED(new StandardMetricDescriptor<ProcessGroupStatus>("bytesTransferred",
        "Bytes Transferred (5 mins)",
        "The total number of bytes read from or written to Content Repository by Processors in this Process Group in the past 5 minutes",
        Formatter.DATA_SIZE,
        s -> s.getBytesRead() + s.getBytesWritten())),

    INPUT_BYTES(new StandardMetricDescriptor<ProcessGroupStatus>("inputBytes",
        "Bytes In (5 mins)",
        "The cumulative size of all FlowFiles that have entered this Process Group via its Input Ports in the past 5 minutes",
        Formatter.DATA_SIZE,
        s -> s.getInputContentSize())),

    INPUT_COUNT(new StandardMetricDescriptor<ProcessGroupStatus>("inputCount",
        "FlowFiles In (5 mins)",
        "The number of FlowFiles that have entered this Process Group via its Input Ports in the past 5 minutes",
        Formatter.COUNT,
        s -> s.getInputCount().longValue())),

    OUTPUT_BYTES(new StandardMetricDescriptor<ProcessGroupStatus>("outputBytes",
        "Bytes Out (5 mins)",
        "The cumulative size of all FlowFiles that have exited this Process Group via its Output Ports in the past 5 minutes",
        Formatter.DATA_SIZE,
        s -> s.getOutputContentSize())),

    OUTPUT_COUNT(new StandardMetricDescriptor<ProcessGroupStatus>("outputCount",
        "FlowFiles Out (5 mins)",
        "The number of FlowFiles that have exited this Process Group via its Output Ports in the past 5 minutes",
        Formatter.COUNT,
        s -> s.getOutputCount().longValue())),

    QUEUED_BYTES(new StandardMetricDescriptor<ProcessGroupStatus>("queuedBytes",
        "Queued Bytes",
        "The cumulative size of all FlowFiles queued in all Connections of this Process Group",
        Formatter.DATA_SIZE,
        s -> s.getQueuedContentSize())),

    QUEUED_COUNT(new StandardMetricDescriptor<ProcessGroupStatus>("queuedCount",
        "Queued Count",
        "The number of FlowFiles queued in all Connections of this Process Group",
        Formatter.COUNT,
        s -> s.getQueuedCount().longValue())),

    TASK_MILLIS(new StandardMetricDescriptor<ProcessGroupStatus>("taskMillis",
        "Total Task Duration (5 mins)",
        "The total number of thread-milliseconds that the Processors within this ProcessGroup have used to complete their tasks in the past 5 minutes",
        Formatter.DURATION,
        s -> calculateTaskMillis(s)));

    private MetricDescriptor<ProcessGroupStatus> descriptor;

    private ProcessGroupStatusDescriptor(final MetricDescriptor<ProcessGroupStatus> descriptor) {
        this.descriptor = descriptor;
    }

    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<ProcessGroupStatus> getDescriptor() {
        return descriptor;
    }


    private static long calculateTaskMillis(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStatus : status.getProcessorStatus()) {
            nanos += procStatus.getProcessingNanos();
        }

        for (final ProcessGroupStatus childStatus : status.getProcessGroupStatus()) {
            nanos += calculateTaskMillis(childStatus);
        }

        return TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
    }
}
