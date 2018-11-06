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
package org.apache.nifi.util;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.CounterMetricDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.ProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.ProcessorStatusDescriptor;
import org.apache.nifi.controller.status.history.RemoteProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusSnapshot;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ComponentMetrics {
    private static final Set<MetricDescriptor<?>> PROCESSOR_METRICS;
    private static final Set<MetricDescriptor<?>> CONNECTION_METRICS;
    private static final Set<MetricDescriptor<?>> PROCESS_GROUP_METRICS;
    private static final Set<MetricDescriptor<?>> RPG_METRICS;

    static {
        PROCESSOR_METRICS = new HashSet<>();
        CONNECTION_METRICS = new HashSet<>();
        PROCESS_GROUP_METRICS = new HashSet<>();
        RPG_METRICS = new HashSet<>();

        for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
            PROCESSOR_METRICS.add(descriptor.getDescriptor());
        }

        for (final ConnectionStatusDescriptor descriptor : ConnectionStatusDescriptor.values()) {
            CONNECTION_METRICS.add(descriptor.getDescriptor());
        }

        for (final ProcessGroupStatusDescriptor descriptor : ProcessGroupStatusDescriptor.values()) {
            PROCESS_GROUP_METRICS.add(descriptor.getDescriptor());
        }

        for (final RemoteProcessGroupStatusDescriptor descriptor : RemoteProcessGroupStatusDescriptor.values()) {
            RPG_METRICS.add(descriptor.getDescriptor());
        }
    }


    public static StatusSnapshot createSnapshot(final ProcessorStatus status, final Date timestamp) {
        if (isEmpty(status)) {
            return null;
        }

        final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(PROCESSOR_METRICS);
        snapshot.setTimestamp(timestamp);

        for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
            if (descriptor.isVisible()) {
                snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
            }
        }

        final Map<String, Long> counters = status.getCounters();
        if (counters != null) {
            for (final Map.Entry<String, Long> entry : counters.entrySet()) {
                final String counterName = entry.getKey();

                final String label = entry.getKey() + " (5 mins)";
                final MetricDescriptor<ProcessorStatus> metricDescriptor = new CounterMetricDescriptor<>(entry.getKey(), label, label, MetricDescriptor.Formatter.COUNT,
                        s -> s.getCounters() == null ? null : s.getCounters().get(counterName));

                snapshot.addCounterStatusMetric(metricDescriptor, entry.getValue());
            }
        }

        return snapshot;
    }

    public static boolean isEmpty(final ProcessorStatus status) {
        for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
            if (descriptor.isVisible()) {
                final Long value = descriptor.getDescriptor().getValueFunction().getValue(status);
                if (value != null && value > 0) {
                    return false;
                }
            }
        }

        return true;
    }


    public static StatusSnapshot createSnapshot(final ConnectionStatus status,  final Date timestamp) {
        if (isEmpty(status)) {
            return null;
        }

        final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(CONNECTION_METRICS);
        snapshot.setTimestamp(timestamp);

        for (final ConnectionStatusDescriptor descriptor : ConnectionStatusDescriptor.values()) {
            snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
        }

        return snapshot;
    }

    public static boolean isEmpty(final ConnectionStatus status) {
        for (final ConnectionStatusDescriptor descriptor : ConnectionStatusDescriptor.values()) {
            final Long value = descriptor.getDescriptor().getValueFunction().getValue(status);
            if (value != null && value > 0) {
                return false;
            }
        }

        return true;
    }

    public static StatusSnapshot createSnapshot(final ProcessGroupStatus status,  final Date timestamp) {
        if (isEmpty(status)) {
            return null;
        }

        final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(PROCESS_GROUP_METRICS);
        snapshot.setTimestamp(timestamp);

        for (final ProcessGroupStatusDescriptor descriptor : ProcessGroupStatusDescriptor.values()) {
            snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
        }

        return snapshot;
    }

    private static boolean isEmpty(final ProcessGroupStatus status) {
        for (final ProcessGroupStatusDescriptor descriptor : ProcessGroupStatusDescriptor.values()) {
            final Long value = descriptor.getDescriptor().getValueFunction().getValue(status);
            if (value != null && value > 0) {
                return false;
            }
        }

        return true;
    }

    public static StatusSnapshot createSnapshot(final RemoteProcessGroupStatus status, final Date timestamp) {
        if (isEmpty(status)) {
            return null;
        }

        final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(RPG_METRICS);
        snapshot.setTimestamp(timestamp);

        for (final RemoteProcessGroupStatusDescriptor descriptor : RemoteProcessGroupStatusDescriptor.values()) {
            snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
        }

        return snapshot;
    }

    private static boolean isEmpty(final RemoteProcessGroupStatus status) {
        for (final RemoteProcessGroupStatusDescriptor descriptor : RemoteProcessGroupStatusDescriptor.values()) {
            final Long value = descriptor.getDescriptor().getValueFunction().getValue(status);
            if (value != null && value > 0) {
                return false;
            }
        }

        return true;
    }
}
