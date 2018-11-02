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
package org.apache.nifi.reporting.gcp;

import java.util.Arrays;
import java.util.List;

public interface GCPMetrics {

    GCPMetric FF_QUEUED = new GCPMetric("flow_files_queued", "Number of flow files queued");
    GCPMetric FF_RECEIVED = new GCPMetric("flow_files_received", "Number of flow files received during the last 5 minutes");
    GCPMetric FF_SENT = new GCPMetric("flow_files_sent", "Number of flow files sent during the last 5 minutes");
    GCPMetric ACTIVE_THREADS = new GCPMetric("active_thread_count", "Number of active threads");
    GCPMetric BYTES_QUEUED = new GCPMetric("bytes_queued", "Number of bytes queued");
    GCPMetric BYTES_RECEIVED = new GCPMetric("bytes_received", "Number of bytes received during the last 5 minutes");
    GCPMetric BYTES_SENT = new GCPMetric("bytes_sent", "Number of bytes sent during the last 5 minutes");
    GCPMetric BYTES_READ = new GCPMetric("bytes_read", "Number of bytes read during the last 5 minutes");
    GCPMetric BYTES_WRITTEN = new GCPMetric("bytes_written", "Number of bytes written during the last 5 minutes");
    GCPMetric TASK_DURATION_SEC = new GCPMetric("total_task_duration_seconds", "Total task duration in seconds over the last 5 minutes");
    GCPMetric TASK_DURATION_NANOSEC = new GCPMetric("total_task_duration_nanoseconds", "Total task duration in nanoseconds over the last 5 minutes");
    GCPMetric BULLETINS = new GCPMetric("bulletins", "Number of bulletins over the last 5 minutes");
    GCPMetric JVM_UPTIME = new GCPMetric("jvm_uptime", "JVM Up time");
    GCPMetric JVM_HEAP_USED = new GCPMetric("jvm_heap_used", "JVM Heap used");
    GCPMetric JVM_HEAP_USAGE = new GCPMetric("jvm_heap_usage", "JVM Heap usage");
    GCPMetric JVM_NON_HEAP_USAGE = new GCPMetric("jvm_non_heap_usage", "JVM Non heap usage");
    GCPMetric JVM_THREAD_STATES_RUNNABLE = new GCPMetric("jvm_thread_runnable", "Number of JVM threads in RUNNABLE state");
    GCPMetric JVM_THREAD_STATES_BLOCKED = new GCPMetric("jvm_thread_blocked", "Number of JVM threads in BLOCKED state");
    GCPMetric JVM_THREAD_STATES_TIMED_WAITING = new GCPMetric("jvm_thread_timed_waiting", "Number of JVM threads in TIMED_WAITING state");
    GCPMetric JVM_THREAD_STATES_TERMINATED = new GCPMetric("jvm_thread_terminated", "Number of JVM threads in TERMINATED state");
    GCPMetric JVM_THREAD_COUNT = new GCPMetric("jvm_thread_count", "Number of JVM theads");
    GCPMetric JVM_DAEMON_THREAD_COUNT = new GCPMetric("jvm_daemon_thread", "Number of JVM daemon threads");
    GCPMetric JVM_FILE_DESCRIPTOR = new GCPMetric("jvm_file_descriptor", "Number of JVM files descriptor");
    GCPMetric JVM_GC_RUNS = new GCPMetric("jvm_gc_runs", "Number of JVM GC runs for ");
    GCPMetric JVM_GC_TIME = new GCPMetric("jvm_gc_duration", "Duration of JVM GCs for ");

    List<GCPMetric> INT_METRICS = Arrays.asList(FF_QUEUED, FF_RECEIVED, FF_SENT, ACTIVE_THREADS, BYTES_QUEUED, BYTES_RECEIVED,
            BYTES_SENT, BYTES_READ, BYTES_WRITTEN, TASK_DURATION_SEC, TASK_DURATION_NANOSEC, BULLETINS, JVM_UPTIME,
            JVM_THREAD_STATES_RUNNABLE, JVM_THREAD_STATES_BLOCKED, JVM_THREAD_STATES_TIMED_WAITING, JVM_THREAD_STATES_TERMINATED,
            JVM_THREAD_COUNT, JVM_DAEMON_THREAD_COUNT);
    List<GCPMetric> DOUBLE_METRICS = Arrays.asList(JVM_HEAP_USED, JVM_HEAP_USAGE, JVM_NON_HEAP_USAGE, JVM_FILE_DESCRIPTOR);

}
