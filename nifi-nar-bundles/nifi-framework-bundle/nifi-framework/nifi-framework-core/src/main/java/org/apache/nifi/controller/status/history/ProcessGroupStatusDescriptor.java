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

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;

import java.util.concurrent.TimeUnit;

public enum ProcessGroupStatusDescriptor {

    BYTES_READ(
        "bytesRead",
        "Bytes Read (5 mins)",
        "The total number of bytes read from Content Repository by Processors in this Process Group in the past 5 minutes",
        Formatter.DATA_SIZE,
        ProcessGroupStatus::getBytesRead),

    BYTES_WRITTEN(
        "bytesWritten",
        "Bytes Written (5 mins)",
        "The total number of bytes written to Content Repository by Processors in this Process Group in the past 5 minutes",
        Formatter.DATA_SIZE,
        ProcessGroupStatus::getBytesWritten),

    BYTES_TRANSFERRED(
        "bytesTransferred",
        "Bytes Transferred (5 mins)",
        "The total number of bytes read from or written to Content Repository by Processors in this Process Group in the past 5 minutes",
        Formatter.DATA_SIZE,
        s -> s.getBytesRead() + s.getBytesWritten()),

    INPUT_BYTES("inputBytes",
        "Bytes In (5 mins)",
        "The cumulative size of all FlowFiles that have entered this Process Group via its Input Ports in the past 5 minutes",
        Formatter.DATA_SIZE,
        ProcessGroupStatus::getInputContentSize),

    INPUT_COUNT(
        "inputCount",
        "FlowFiles In (5 mins)",
        "The number of FlowFiles that have entered this Process Group via its Input Ports in the past 5 minutes",
        Formatter.COUNT,
        s -> s.getInputCount().longValue()),

    OUTPUT_BYTES(
        "outputBytes",
        "Bytes Out (5 mins)",
        "The cumulative size of all FlowFiles that have exited this Process Group via its Output Ports in the past 5 minutes",
        Formatter.DATA_SIZE,
        ProcessGroupStatus::getOutputContentSize),

    OUTPUT_COUNT(
        "outputCount",
        "FlowFiles Out (5 mins)",
        "The number of FlowFiles that have exited this Process Group via its Output Ports in the past 5 minutes",
        Formatter.COUNT,
        s -> s.getOutputCount().longValue()),

    QUEUED_BYTES(
        "queuedBytes",
        "Queued Bytes",
        "The cumulative size of all FlowFiles queued in all Connections of this Process Group",
        Formatter.DATA_SIZE,
        ProcessGroupStatus::getQueuedContentSize),

    QUEUED_COUNT(
        "queuedCount",
        "Queued Count",
        "The number of FlowFiles queued in all Connections of this Process Group",
        Formatter.COUNT,
        s -> s.getQueuedCount().longValue()),

    TASK_MILLIS(
        "taskMillis",
        "Total Task Duration (5 mins)",
        "The total number of thread-milliseconds that the Processors within this ProcessGroup have used to complete their tasks in the past 5 minutes",
        Formatter.DURATION,
        ProcessGroupStatusDescriptor::calculateTaskMillis);


    private MetricDescriptor<ProcessGroupStatus> descriptor;

    ProcessGroupStatusDescriptor(final String field, final String label, final String description,
                               final MetricDescriptor.Formatter formatter, final ValueMapper<ProcessGroupStatus> valueFunction) {

        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction);
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
