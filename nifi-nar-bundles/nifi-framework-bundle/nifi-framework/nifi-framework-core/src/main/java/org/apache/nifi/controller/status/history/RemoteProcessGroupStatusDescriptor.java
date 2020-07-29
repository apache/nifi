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

import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;

import java.util.List;
import java.util.concurrent.TimeUnit;

public enum RemoteProcessGroupStatusDescriptor {
    SENT_BYTES(
        "sentBytes",
        "Bytes Sent (5 mins)",
        "The cumulative size of all FlowFiles that have been successfully sent to the remote system in the past 5 minutes",
        Formatter.DATA_SIZE,
        RemoteProcessGroupStatus::getSentContentSize),

    SENT_COUNT(
        "sentCount",
        "FlowFiles Sent (5 mins)",
        "The number of FlowFiles that have been successfully sent to the remote system in the past 5 minutes",
        Formatter.COUNT,
        s -> s.getSentCount().longValue()),

    RECEIVED_BYTES(
        "receivedBytes",
        "Bytes Received (5 mins)",
        "The cumulative size of all FlowFiles that have been received from the remote system in the past 5 minutes",
        Formatter.DATA_SIZE,
        RemoteProcessGroupStatus::getReceivedContentSize),

    RECEIVED_COUNT(
        "receivedCount",
        "FlowFiles Received (5 mins)",
        "The number of FlowFiles that have been received from the remote system in the past 5 minutes",
        Formatter.COUNT,
        s -> s.getReceivedCount().longValue()),

    RECEIVED_BYTES_PER_SECOND(
        "receivedBytesPerSecond",
        "Received Bytes Per Second",
        "The data rate at which data was received from the remote system in the past 5 minutes in terms of Bytes Per Second",
        Formatter.DATA_SIZE,
        s -> s.getReceivedContentSize().longValue() / 300L),

    SENT_BYTES_PER_SECOND(
        "sentBytesPerSecond",
        "Sent Bytes Per Second",
        "The data rate at which data was received from the remote system in the past 5 minutes in terms of Bytes Per Second",
        Formatter.DATA_SIZE,
        s -> s.getSentContentSize().longValue() / 300L),

    TOTAL_BYTES_PER_SECOND("totalBytesPerSecond",
        "Total Bytes Per Second",
        "The sum of the send and receive data rate from the remote system in the past 5 minutes in terms of Bytes Per Second",
        Formatter.DATA_SIZE,
        new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return Long.valueOf((status.getReceivedContentSize().longValue() + status.getSentContentSize().longValue()) / 300L);
            }
        }),

    AVERAGE_LINEAGE_DURATION(
        "averageLineageDuration",
        "Average Lineage Duration (5 mins)",
        "The average amount of time that a FlowFile took to process from receipt to drop in the past 5 minutes. For Processors that do not terminate FlowFiles, this value will be 0.",
        Formatter.DURATION,
        s -> s.getAverageLineageDuration(TimeUnit.MILLISECONDS),
        new ValueReducer<StatusSnapshot, Long>() {
            @Override
            public Long reduce(final List<StatusSnapshot> values) {
                long millis = 0L;
                int count = 0;

                for (final StatusSnapshot snapshot : values) {
                    final long sent = snapshot.getStatusMetric(SENT_COUNT.getDescriptor()).longValue();
                    count += sent;

                    final long avgMillis = snapshot.getStatusMetric(AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue();
                    final long totalMillis = avgMillis * sent;
                    millis += totalMillis;
                }

                return count == 0 ? 0 : millis / count;
            }
        });


    private final MetricDescriptor<RemoteProcessGroupStatus> descriptor;

    RemoteProcessGroupStatusDescriptor(final String field, final String label, final String description,
                               final MetricDescriptor.Formatter formatter, final ValueMapper<RemoteProcessGroupStatus> valueFunction) {
        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction);
    }

    RemoteProcessGroupStatusDescriptor(final String field, final String label, final String description,
                                       final MetricDescriptor.Formatter formatter, final ValueMapper<RemoteProcessGroupStatus> valueFunction, final ValueReducer<StatusSnapshot, Long> reducer) {
        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction, reducer);
    }

    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<RemoteProcessGroupStatus> getDescriptor() {
        return descriptor;
    }
}
