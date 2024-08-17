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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.CounterMetricDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.questdb.QueryRowContext;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

final class CounterStatisticsResultProcessor implements QueryResultProcessor<List<StatusSnapshot>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CounterStatisticsResultProcessor.class);

    private final List<StatusSnapshot> processorSnapshots;
    final Map<Long, StatusSnapshot> processorSnapshotsByTime;

    CounterStatisticsResultProcessor(final List<StatusSnapshot> processorSnapshots) {
        this.processorSnapshots = processorSnapshots;
        processorSnapshotsByTime = processorSnapshots.stream().collect(Collectors.toMap(s -> s.getTimestamp().getTime(), s -> s));
    }

    @Override
    public void processRow(final QueryRowContext context) {
        final long counterCreatedAt = TimeUnit.MICROSECONDS.toMillis(context.getTimestamp(0));
        final String counterName = context.getString(2);
        final long counterValue = context.getLong(3);
        final StatusSnapshot processorStatusSnapshot = processorSnapshotsByTime.get(counterCreatedAt);
        final MetricDescriptor<ProcessorStatus> metricDescriptor = getMetricDescriptor(counterName);

        if (processorStatusSnapshot instanceof StandardStatusSnapshot) {
            ((StandardStatusSnapshot) processorStatusSnapshot).addStatusMetric(metricDescriptor, counterValue);
        } else {
            LOGGER.warn("The snapshot is not an instance of StandardStatusSnapshot");
        }
    }

    @Override
    public List<StatusSnapshot> getResult() {
        return processorSnapshots;
    }

    private static CounterMetricDescriptor<ProcessorStatus> getMetricDescriptor(final String counterName) {
        return new CounterMetricDescriptor<>(
            counterName,
            counterName + " (5 mins)",
            counterName + " (5 mins)",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getCounters() == null ? null : s.getCounters().get(counterName)
        );
    }
}
