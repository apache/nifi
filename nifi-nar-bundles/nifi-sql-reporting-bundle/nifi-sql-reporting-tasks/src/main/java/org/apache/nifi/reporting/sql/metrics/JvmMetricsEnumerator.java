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

package org.apache.nifi.reporting.sql.metrics;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.reporting.ReportingContext;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.State.BLOCKED;
import static java.lang.Thread.State.RUNNABLE;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;

public class JvmMetricsEnumerator implements Enumerator<Object> {
    private final ReportingContext context;
    private final ComponentLog logger;
    private final int[] fields;

    private volatile JvmMetrics virtualMachineMetrics;
    private Object currentRow;
    private boolean fetchMetrics = true;

    public JvmMetricsEnumerator(final ReportingContext context, final ComponentLog logger, final int[] fields) {
        this.context = context;
        this.logger = logger;
        this.fields = fields;
        reset();
        virtualMachineMetrics = JmxJvmMetrics.getInstance();
    }

    @Override
    public Object current() {
        return currentRow;
    }

    @Override
    public boolean moveNext() {
        if (fetchMetrics) {
            currentRow = filterColumns(virtualMachineMetrics);
            // Toggle fetchMetrics so only one row is returned
            fetchMetrics = false;
            return true;
        } else {
            // If we are out of data, close the InputStream. We do this because
            // Calcite does not necessarily call our close() method.
            close();
            try {
                onFinish();
            } catch (final Exception e) {
                logger.error("Failed to perform tasks when enumerator was finished", e);
            }

            // Reset fetchMetrics for the next scan
            fetchMetrics = true;
            return false;
        }
    }

    protected int getRecordsRead() {
        return 1;
    }

    protected void onFinish() {
    }

    private Object filterColumns(final JvmMetrics metrics) {
        if (metrics == null) {
            return null;
        }

        final ArrayList<Object> rowList = new ArrayList<>();
        rowList.add(virtualMachineMetrics.daemonThreadCount());
        rowList.add(virtualMachineMetrics.threadCount());

        // Normalize the thread percentages to an integer 0-100
        Double d = virtualMachineMetrics.threadStatePercentages().get(BLOCKED);
        rowList.add((int) (100 * (d == null ? 0 : d)));
        d = virtualMachineMetrics.threadStatePercentages().get(RUNNABLE);
        rowList.add((int) (100 * (d == null ? 0 : d)));
        d = virtualMachineMetrics.threadStatePercentages().get(TERMINATED);
        rowList.add((int) (100 * (d == null ? 0 : d)));
        d = virtualMachineMetrics.threadStatePercentages().get(TIMED_WAITING);
        rowList.add((int) (100 * (d == null ? 0 : d)));

        rowList.add(virtualMachineMetrics.uptime());
        rowList.add(virtualMachineMetrics.heapUsed(DataUnit.B));
        rowList.add(virtualMachineMetrics.heapUsage());
        rowList.add(virtualMachineMetrics.nonHeapUsage());
        rowList.add(virtualMachineMetrics.fileDescriptorUsage());
        virtualMachineMetrics.garbageCollectors().values().forEach((gc) -> {
            rowList.add(gc.getRuns());
            rowList.add(gc.getTime(TimeUnit.MILLISECONDS));
        });

        final Object[] row = rowList.toArray();

        // If we want no fields just return null
        if (fields == null) {
            return row;
        }

        // If we want only a single field, then Calcite is going to expect us to return
        // the actual value, NOT a 1-element array of values.
        if (fields.length == 1) {
            final int desiredCellIndex = fields[0];
            return row[desiredCellIndex];
        }

        // Create a new Object array that contains only the desired fields.
        final Object[] filtered = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final int indexToKeep = fields[i];
            filtered[i] = row[indexToKeep];
        }

        return filtered;
    }

    @Override
    public void reset() {
        // Reset fetchMetrics for the next scan
        fetchMetrics = true;
    }

    @Override
    public void close() {
    }
}
