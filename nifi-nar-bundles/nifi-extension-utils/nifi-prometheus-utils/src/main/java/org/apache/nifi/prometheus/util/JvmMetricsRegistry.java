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
package org.apache.nifi.prometheus.util;

import io.prometheus.client.Gauge;

public class JvmMetricsRegistry extends AbstractMetricsRegistry {

    public JvmMetricsRegistry() {

        ///////////////////////////////////////////////////////////////
        // JVM Metrics
        ///////////////////////////////////////////////////////////////
        nameToGaugeMap.put("JVM_HEAP_INIT", Gauge.build()
                .name("nifi_jvm_heap_init")
                .help("The amount of heap memory in bytes that the JVM initially requested from the operating system for memory management, returning -1 if undefined.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_COMMITTED", Gauge.build()
                .name("nifi_jvm_heap_committed")
                .help("The amount of heap memory in bytes that is committed and guaranteed for the JVM to use.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_MAX", Gauge.build()
                .name("nifi_jvm_heap_max")
                .help("The maximum amount of heap memory in bytes that can be used for memory management, returning -1 if undefined.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_USED", Gauge.build()
                .name("nifi_jvm_heap_used")
                .help("NiFi JVM heap used")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_USAGE", Gauge.build()
                .name("nifi_jvm_heap_usage")
                .help("NiFi JVM heap usage")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_NON_INIT", Gauge.build()
                .name("nifi_jvm_heap_non_init")
                .help("The amount of non-heap memory in bytes that the JVM initially requested from the operating system for memory management, returning -1 if undefined.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_NON_COMMITTED", Gauge.build()
                .name("nifi_jvm_heap_non_committed")
                .help("The amount of non-heap memory in bytes that is committed and guaranteed for the JVM to use.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_NON_MAX", Gauge.build()
                .name("nifi_jvm_heap_non_max")
                .help("The maximum amount of non-heap memory in bytes that can be used for memory management, returning -1 if undefined.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_NON_USED", Gauge.build()
                .name("nifi_jvm_heap_non_used")
                .help("The amount of used non-heap memory in bytes.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_HEAP_NON_USAGE", Gauge.build()
                .name("nifi_jvm_heap_non_usage")
                .help("NiFi JVM non-heap usage")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_THREAD_COUNT", Gauge.build()
                .name("nifi_jvm_thread_count")
                .help("NiFi JVM thread count")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_DAEMON_THREAD_COUNT", Gauge.build()
                .name("nifi_jvm_daemon_thread_count")
                .help("NiFi JVM daemon thread count")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_DEADLOCKED_THREAD_COUNT", Gauge.build()
                .name("nifi_jvm_deadlocked_thread_count")
                .help("NiFi JVM deadlocked thread count")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_UPTIME", Gauge.build()
                .name("nifi_jvm_uptime")
                .help("NiFi JVM uptime")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_FILE_DESCRIPTOR_USAGE", Gauge.build()
                .name("nifi_jvm_file_descriptor_usage")
                .help("NiFi JVM file descriptor usage")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("JVM_GC_RUNS", Gauge.build()
                .name("nifi_jvm_gc_runs")
                .help("NiFi JVM GC number of runs")
                .labelNames("instance", "gc_name")
                .register(registry));

        nameToGaugeMap.put("JVM_GC_TIME", Gauge.build()
                .name("nifi_jvm_gc_time")
                .help("NiFi JVM GC time in milliseconds")
                .labelNames("instance", "gc_name")
                .register(registry));
    }

}
