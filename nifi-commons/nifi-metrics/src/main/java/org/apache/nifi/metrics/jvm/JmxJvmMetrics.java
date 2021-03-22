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
package org.apache.nifi.metrics.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.nifi.processor.DataUnit;

import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class JmxJvmMetrics implements JvmMetrics {

    static final String REGISTRY_METRICSET_JVM_ATTRIBUTES = "jvm-attributes";
    static final String REGISTRY_METRICSET_MEMORY = "memory";
    public static final String MEMORY_POOLS = REGISTRY_METRICSET_MEMORY + ".pools";
    static final String REGISTRY_METRICSET_THREADS = "threads";
    static final String REGISTRY_METRICSET_GARBAGE_COLLECTORS = "garbage-collectors";
    static final String JVM_ATTRIBUTES_NAME = REGISTRY_METRICSET_JVM_ATTRIBUTES + ".name";
    static final String JVM_ATTRIBUTES_UPTIME = REGISTRY_METRICSET_JVM_ATTRIBUTES + ".uptime";
    static final String JVM_ATTRIBUTES_VENDOR = REGISTRY_METRICSET_JVM_ATTRIBUTES + ".vendor";
    static final String MEMORY_TOTAL_INIT = REGISTRY_METRICSET_MEMORY + ".total.init";
    static final String MEMORY_TOTAL_USED = REGISTRY_METRICSET_MEMORY + ".total.used";
    static final String MEMORY_TOTAL_MAX = REGISTRY_METRICSET_MEMORY + ".total.max";
    static final String MEMORY_TOTAL_COMMITTED = REGISTRY_METRICSET_MEMORY + ".total.committed";
    static final String MEMORY_HEAP_INIT = REGISTRY_METRICSET_MEMORY + ".heap.init";
    static final String MEMORY_HEAP_USED = REGISTRY_METRICSET_MEMORY + ".heap.used";
    static final String MEMORY_HEAP_MAX = REGISTRY_METRICSET_MEMORY + ".heap.max";
    static final String MEMORY_HEAP_COMMITTED = REGISTRY_METRICSET_MEMORY + ".heap.committed";
    static final String MEMORY_HEAP_USAGE = REGISTRY_METRICSET_MEMORY + ".heap.usage";
    static final String MEMORY_NON_HEAP_USAGE = REGISTRY_METRICSET_MEMORY + ".non-heap.usage";
    static final String THREADS_COUNT = REGISTRY_METRICSET_THREADS + ".count";
    static final String THREADS_DAEMON_COUNT = REGISTRY_METRICSET_THREADS + ".daemon.count";
    static final String THREADS_DEADLOCKS = REGISTRY_METRICSET_THREADS + ".deadlocks";
    static final String OS_FILEDESCRIPTOR_USAGE = "os.filedescriptor.usage";

    private static AtomicReference<MetricRegistry> metricRegistry = new AtomicReference<>(null);

    private JmxJvmMetrics() {
    }

    public static JmxJvmMetrics getInstance() {
        if (metricRegistry.get() == null) {
            metricRegistry.set(new MetricRegistry());
            metricRegistry.get().register(REGISTRY_METRICSET_JVM_ATTRIBUTES, new JvmAttributeGaugeSet());
            metricRegistry.get().register(REGISTRY_METRICSET_MEMORY, new MemoryUsageGaugeSet());
            metricRegistry.get().register(REGISTRY_METRICSET_THREADS, new ThreadStatesGaugeSet());
            metricRegistry.get().register(REGISTRY_METRICSET_GARBAGE_COLLECTORS, new GarbageCollectorMetricSet());
            metricRegistry.get().register(OS_FILEDESCRIPTOR_USAGE, new FileDescriptorRatioGauge());

        }
        return new JmxJvmMetrics();
    }

    private Object getMetric(String metricName) {
        final SortedMap<String, Gauge> gauges = metricRegistry.get().getGauges((name, metric) -> name.equals(metricName));
        if (gauges.isEmpty()) {
            throw new IllegalArgumentException(String.format("Unable to retrieve metric \"%s\"", metricName));
        }
        return gauges.get(metricName).getValue();
    }

    public Set<String> getMetricNames(String metricNamePrefix) {
        if (metricNamePrefix == null || metricNamePrefix.length() == 0) {
            throw new IllegalArgumentException("A metric name prefix must be supplied");
        }
        final String normalizedMetricNamePrefix = metricNamePrefix.endsWith(".") ? metricNamePrefix : metricNamePrefix + '.';
        return metricRegistry.get().getNames().stream()
                .filter(name -> name.startsWith(normalizedMetricNamePrefix))
                .map(name -> name.substring(normalizedMetricNamePrefix.length(), name.indexOf(".", normalizedMetricNamePrefix.length())))
                .collect(Collectors.toSet());
    }

    @Override
    public double totalInit(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_TOTAL_INIT), DataUnit.B);
    }

    @Override
    public double totalUsed(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_TOTAL_USED), DataUnit.B);
    }

    @Override
    public double totalMax(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_TOTAL_MAX), DataUnit.B);
    }

    @Override
    public double totalCommitted(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_TOTAL_COMMITTED), DataUnit.B);
    }

    @Override
    public double heapInit(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_HEAP_INIT), DataUnit.B);
    }

    @Override
    public double heapUsed(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_HEAP_USED), DataUnit.B);
    }

    @Override
    public double heapMax(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_HEAP_MAX), DataUnit.B);
    }

    @Override
    public double heapCommitted(DataUnit dataUnit) {
        return (dataUnit == null ? DataUnit.B : dataUnit).convert((Long) getMetric(MEMORY_HEAP_COMMITTED), DataUnit.B);
    }

    @Override
    public double heapUsage() {
        double usage = (Double) getMetric(MEMORY_HEAP_USAGE);
        return usage < 0 ? -1.0 : usage;
    }

    @Override
    public double nonHeapUsage() {
        double usage = (Double) getMetric(MEMORY_NON_HEAP_USAGE);
        return usage < 0 ? -1.0 : usage;
    }

    @Override
    public Map<String, Double> memoryPoolUsage() {
        Set<String> poolNames = getMetricNames(MEMORY_POOLS);
        Map<String, Double> memoryPoolUsage = new HashMap<>();
        for (String poolName : poolNames) {
            memoryPoolUsage.put(poolName, (Double) getMetric(MEMORY_POOLS + "." + poolName + ".usage"));
        }
        return Collections.unmodifiableMap(memoryPoolUsage);
    }

    @Override
    public double fileDescriptorUsage() {
        return (Double) getMetric(OS_FILEDESCRIPTOR_USAGE);
    }

    @Override
    public String version() {
        return (String) getMetric(JVM_ATTRIBUTES_VENDOR);
    }

    @Override
    public String name() {
        return (String) getMetric(JVM_ATTRIBUTES_NAME);
    }

    @Override
    public long uptime() {
        return TimeUnit.MILLISECONDS.toSeconds((Long) getMetric(JVM_ATTRIBUTES_UPTIME));
    }

    @Override
    public int threadCount() {
        return (Integer) getMetric(THREADS_COUNT);
    }

    @Override
    public int daemonThreadCount() {
        return (Integer) getMetric(THREADS_DAEMON_COUNT);
    }

    @Override
    public Map<String, GarbageCollectorStats> garbageCollectors() {
        Set<String> garbageCollectors = getMetricNames(REGISTRY_METRICSET_GARBAGE_COLLECTORS);
        Map<String, GarbageCollectorStats> gcStats = new HashMap<>();
        for (String garbageCollector : garbageCollectors) {
            gcStats.put(garbageCollector, new GarbageCollectorStats(
                    (Long) getMetric(REGISTRY_METRICSET_GARBAGE_COLLECTORS + "." + garbageCollector + ".count"),
                    (Long) getMetric(REGISTRY_METRICSET_GARBAGE_COLLECTORS + "." + garbageCollector + ".time")));
        }
        return Collections.unmodifiableMap(gcStats);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> deadlockedThreads() {
        return (Set<String>) getMetric(THREADS_DEADLOCKS);
    }

    @Override
    public Map<Thread.State, Double> threadStatePercentages() {
        int totalThreadCount = (Integer) getMetric(THREADS_COUNT);
        final Map<Thread.State, Double> threadStatePercentages = new HashMap<Thread.State, Double>();
        for (Thread.State state : Thread.State.values()) {
            threadStatePercentages.put(state, (Integer) getMetric(REGISTRY_METRICSET_THREADS + "." + state.name().toLowerCase() + ".count") / (double) totalThreadCount);
        }
        return Collections.unmodifiableMap(threadStatePercentages);
    }

    @Override
    public void threadDump(OutputStream out) {
        throw new UnsupportedOperationException("This operation has not yet been implemented");
    }

    @Override
    public Map<String, BufferPoolStats> getBufferPoolStats() {
        throw new UnsupportedOperationException("This operation has not yet been implemented");
    }
}