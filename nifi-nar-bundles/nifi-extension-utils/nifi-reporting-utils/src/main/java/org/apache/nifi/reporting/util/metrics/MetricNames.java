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
package org.apache.nifi.reporting.util.metrics;

/**
 * The Metric names to send to Ambari.
 */
public interface MetricNames {

    // Metric Name separator
    String METRIC_NAME_SEPARATOR = ".";

    // NiFi Metrics
    String FLOW_FILES_RECEIVED = "FlowFilesReceivedLast5Minutes";
    String BYTES_RECEIVED = "BytesReceivedLast5Minutes";
    String FLOW_FILES_SENT = "FlowFilesSentLast5Minutes";
    String BYTES_SENT = "BytesSentLast5Minutes";
    String FLOW_FILES_QUEUED = "FlowFilesQueued";
    String BYTES_QUEUED = "BytesQueued";
    String BYTES_READ = "BytesReadLast5Minutes";
    String BYTES_WRITTEN = "BytesWrittenLast5Minutes";
    String ACTIVE_THREADS = "ActiveThreads";
    String TOTAL_TASK_DURATION_SECONDS = "TotalTaskDurationSeconds";
    String TOTAL_TASK_DURATION_NANOS = "TotalTaskDurationNanoSeconds";

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

    // OS Metrics
    String LOAD1MN = "loadAverage1min";
    String CORES = "availableCores";

}
