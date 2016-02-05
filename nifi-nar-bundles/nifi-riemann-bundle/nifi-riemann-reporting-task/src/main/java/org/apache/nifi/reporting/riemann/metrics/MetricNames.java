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
package org.apache.nifi.reporting.riemann.metrics;

import java.util.List;

import com.google.common.collect.Lists;

public interface MetricNames {
    // Service Suffix
    String SERVICE_SUFFIX = "last-5-minutes";

    // NiFi Metrics
    String FLOW_FILES_RECEIVED = "flowfiles.received";
    String FLOW_FILES_SENT = "flowfiles.sent";
    String FLOW_FILES_TRANSFERRED = "flowfiles.transferred";
    String FLOW_FILES_REMOVED = "flowfiles.removed";
    String FLOW_FILES_QUEUED = "flowfiles.queued.count";
    String BYTES_RECEIVED = "bytes.received";
    String BYTES_SENT = "bytes.sent";
    String BYTES_QUEUED = "bytes.queued";
    String BYTES_READ = "bytes.read";
    String BYTES_WRITTEN = "bytes.written";
    String BYTES_TRANSFERRED = "bytes.transferred";
    String ACTIVE_THREADS = "active.threads";
    String TOTAL_TASK_DURATION = "total.task.duration.seconds";
    String AVERAGE_LINEAGE_DURATION = "average.lineage.duration.ms";
    String INPUT_CONTENT_SIZE = "input.content.size";
    String OUTPUT_CONTENT_SIZE = "output.content.size";
    String INPUT_BYTES = "input.bytes";
    String OUTPUT_BYTES = "output.bytes";

    // JVM Metrics
    String JVM_UPTIME = "jvm.uptime";
    String JVM_HEAP_USED = "jvm.heap_used";
    String JVM_HEAP_USAGE = "jvm.heap_usage";
    String JVM_NON_HEAP_USAGE = "jvm.non_heap_usage";
    String JVM_THREAD_STATES_RUNNABLE = "jvm.thread_states.runnable";
    String JVM_THREAD_STATES_BLOCKED = "jvm.thread_states.blocked";
    String JVM_THREAD_STATES_TIMED_WAITING = "jvm.thread_states.timed_waiting";
    String JVM_THREAD_STATES_TERMINATED = "jvm.thread_states.terminated";
    String JVM_THREAD_COUNT = "jvm.thread_count";
    String JVM_DAEMON_THREAD_COUNT = "jvm.daemon_thread_count";
    String JVM_FILE_DESCRIPTOR_USAGE = "jvm.file_descriptor_usage";
    String JVM_GC_RUNS = "jvm.gc.runs";
    String JVM_GC_TIME = "jvm.gc.time";

    List<String> PROCESS_GROUP_METRICS = Lists.newArrayList(FLOW_FILES_RECEIVED, FLOW_FILES_SENT, FLOW_FILES_TRANSFERRED, FLOW_FILES_QUEUED, BYTES_RECEIVED, BYTES_SENT, BYTES_QUEUED, BYTES_READ,
            BYTES_WRITTEN, BYTES_TRANSFERRED, ACTIVE_THREADS, TOTAL_TASK_DURATION);

    List<String> PROCESSOR_METRICS = Lists.newArrayList(FLOW_FILES_RECEIVED, FLOW_FILES_SENT, FLOW_FILES_REMOVED, BYTES_RECEIVED, BYTES_SENT, BYTES_QUEUED, BYTES_READ, BYTES_WRITTEN, ACTIVE_THREADS,
            AVERAGE_LINEAGE_DURATION, INPUT_BYTES, OUTPUT_BYTES);

    List<String> JVM_METRICS = Lists.newArrayList(JVM_DAEMON_THREAD_COUNT, JVM_FILE_DESCRIPTOR_USAGE, JVM_GC_RUNS, JVM_GC_TIME, JVM_HEAP_USAGE, JVM_HEAP_USED, JVM_THREAD_COUNT,
            JVM_THREAD_STATES_BLOCKED, JVM_THREAD_STATES_RUNNABLE, JVM_THREAD_STATES_TERMINATED, JVM_THREAD_STATES_TIMED_WAITING, JVM_UPTIME);

}
