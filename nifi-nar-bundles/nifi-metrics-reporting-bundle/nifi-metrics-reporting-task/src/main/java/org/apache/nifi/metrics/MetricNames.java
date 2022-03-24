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
package org.apache.nifi.metrics;

/**
 * The Metric names to send to Ambari.
 */
public interface MetricNames {

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
    String TOTAL_TASK_DURATION_NANOS = "TotalTaskDurationNanoSeconds";
}
