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
package org.apache.nifi.reporting.azure.loganalytics;

/**
 * The Metric names to send to Azure Log Analytics.
 */
public interface MetricNames {

    // Metric Name separator
    String METRIC_NAME_SEPARATOR = ".";

    // NiFi Metrics
    String FLOW_FILES_RECEIVED = "FlowFilesReceived";
    String FLOW_FILES_TRANSFERRED = "FlowFilesTransferred";
    String BYTES_RECEIVED = "BytesReceived";
    String FLOW_FILES_SENT = "FlowFilesSent";
    String BYTES_SENT = "BytesSent";
    String FLOW_FILES_QUEUED = "FlowFilesQueued";
    String BYTES_TRANSFERRED= "BytesTransferred";
    String BYTES_QUEUED= "BytesQueued";
    String BYTES_READ = "BytesRead";
    String BYTES_WRITTEN = "BytesWritten";
    String ACTIVE_THREADS = "ActiveThreads";
    String TOTAL_TASK_DURATION_SECONDS = "TotalTaskDurationSeconds";
    String TOTAL_TASK_DURATION_NANOS = "TotalTaskDurationNanoSeconds";
    String OUTPUT_CONTENT_SIZE = "OutputContentSize";
    String INPUT_CONTENT_SIZE = "InputContentSize";
    String QUEUED_CONTENT_SIZE = "QueuedContentSize";
    String OUTPUT_COUNT = "OutputCount";
    String INPUT_COUNT = "InputCount";
    String QUEUED_COUNT = "QueuedCount";
    String OUTPUT_BYTES = "OutputBytes";
    String INPUT_BYTES = "InputBytes";
    String QUEUED_BYTES = "QueuedBytes";

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

}
