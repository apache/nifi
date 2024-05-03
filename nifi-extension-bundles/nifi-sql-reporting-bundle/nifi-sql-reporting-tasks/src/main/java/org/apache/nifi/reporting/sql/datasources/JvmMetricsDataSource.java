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

package org.apache.nifi.reporting.sql.datasources;

import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.State.BLOCKED;
import static java.lang.Thread.State.RUNNABLE;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;

public class JvmMetricsDataSource implements ResettableDataSource {
    private static final NiFiTableSchema BASE_SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema(MetricNames.JVM_DAEMON_THREAD_COUNT.replaceAll("[.-]", "_"), int.class, false),
        new ColumnSchema(MetricNames.JVM_THREAD_COUNT.replaceAll("[.-]", "_"), int.class, false),
        new ColumnSchema(MetricNames.JVM_THREAD_STATES_BLOCKED.replaceAll("[.-]", "_"), int.class, false),
        new ColumnSchema(MetricNames.JVM_THREAD_STATES_RUNNABLE.replaceAll("[.-]", "_"), int.class, false),
        new ColumnSchema(MetricNames.JVM_THREAD_STATES_TERMINATED.replaceAll("[.-]", "_"), int.class, false),
        new ColumnSchema(MetricNames.JVM_THREAD_STATES_TIMED_WAITING.replaceAll("[.-]", "_"), int.class, false),
        new ColumnSchema(MetricNames.JVM_UPTIME.replaceAll("[.-]", "_"), long.class, false),
        new ColumnSchema(MetricNames.JVM_HEAP_USED.replaceAll("[.-]", "_"), double.class, false),
        new ColumnSchema(MetricNames.JVM_HEAP_USAGE.replaceAll("[.-]", "_"), double.class, false),
        new ColumnSchema(MetricNames.JVM_NON_HEAP_USAGE.replaceAll("[.-]", "_"), double.class, false),
        new ColumnSchema(MetricNames.JVM_FILE_DESCRIPTOR_USAGE.replaceAll("[.-]", "_"), double.class, false)
    ));

    private final JvmMetrics virtualMachineMetrics = JmxJvmMetrics.getInstance();
    private final NiFiTableSchema schema;

    public JvmMetricsDataSource() {
        final List<ColumnSchema> columns = new ArrayList<>(BASE_SCHEMA.columns());
        for (final String gcName : virtualMachineMetrics.garbageCollectors().keySet()) {
            final String gcRunsName = normalize(MetricNames.JVM_GC_RUNS + "_" + gcName);
            final String gcTimeName = normalize(MetricNames.JVM_GC_TIME + "_" + gcName);

            columns.add(new ColumnSchema(gcRunsName, long.class, true));
            columns.add(new ColumnSchema(gcTimeName, long.class, true));
        }

        schema = new NiFiTableSchema(columns);
    }

    private String normalize(final String value) {
        return value.replaceAll("[ -.]", "_");
    }

    public NiFiTableSchema getSchema() {
        return schema;
    }

    @Override
    public RowStream reset() {
        final List<Object> values = new ArrayList<>();
        values.add(virtualMachineMetrics.daemonThreadCount());
        values.add(virtualMachineMetrics.threadCount());

        // Normalize the thread percentages to an integer 0-100
        values.add(getThreadStatePercentage(BLOCKED));
        values.add(getThreadStatePercentage(RUNNABLE));
        values.add(getThreadStatePercentage(TERMINATED));
        values.add(getThreadStatePercentage(TIMED_WAITING));

        values.add(virtualMachineMetrics.uptime());
        values.add(virtualMachineMetrics.heapUsed(DataUnit.B));
        values.add(virtualMachineMetrics.heapUsage());
        values.add(virtualMachineMetrics.nonHeapUsage());
        values.add(virtualMachineMetrics.fileDescriptorUsage());

        virtualMachineMetrics.garbageCollectors().values().forEach((gc) -> {
            values.add(gc.getRuns());
            values.add(gc.getTime(TimeUnit.MILLISECONDS));
        });

        return new IterableRowStream<>(Collections.singleton(values), v -> v.toArray(new Object[0]));
    }

    private int getThreadStatePercentage(final Thread.State threadState) {
        final Double ratio = virtualMachineMetrics.threadStatePercentages().get(threadState);
        if (ratio == null) {
            return 0;
        }

        return (int) (ratio * 100D);
    }
}
