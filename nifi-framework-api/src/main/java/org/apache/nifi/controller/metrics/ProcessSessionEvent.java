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
package org.apache.nifi.controller.metrics;

import java.util.Map;

/**
 * Event abstraction for capturing metrics produced during ProcessSession operations
 */
public interface ProcessSessionEvent {
    /**
     * Get Component Metric Context describing the Component associated with the recorded ProcessSession operations
     *
     * @return Component Metric Context
     */
    ComponentMetricContext getComponentMetricContext();

    /**
     * Get number of invocations for the Component associated with the recorded ProcessSession operations
     *
     * @return Component Invocations
     */
    int getInvocations();

    /**
     * Get FlowFiles handled from input Relationships
     *
     * @return FlowFiles handled from input Relationships
     */
    int getFlowFilesIn();

    /**
     * Get FlowFiles transferred to output Relationships
     *
     * @return FlowFiles transferred to output Relationships
     */
    int getFlowFilesOut();

    /**
     * Get FlowFiles removed from input queues
     *
     * @return FlowFiles removed
     */
    int getFlowFilesRemoved();

    /**
     * Get FlowFiles sent to remote destinations
     *
     * @return FlowFiles sent to remote destinations
     */
    int getFlowFilesSent();

    /**
     * Get FlowFiles received from remote sources
     *
     * @return FlowFiles received from remote sources
     */
    int getFlowFilesReceived();

    /**
     * Get content size in bytes handled from input Relationships
     *
     * @return Content size in bytes from input Relationships
     */
    long getContentSizeIn();

    /**
     * Get content size in bytes transferred to output Relationships
     *
     * @return Content size in bytes transferred to output Relationships
     */
    long getContentSizeOut();

    /**
     * Get content size in bytes of FlowFiles removed from queues
     *
     * @return Content size in bytes of FlowFiles removed from queues
     */
    long getContentSizeRemoved();

    /**
     * Get bytes read from input FlowFiles
     *
     * @return Bytes read
     */
    long getBytesRead();

    /**
     * Get bytes written to output FlowFiles
     *
     * @return Bytes written
     */
    long getBytesWritten();

    /**
     * Get bytes sent to remote destinations
     *
     * @return Bytes sent
     */
    long getBytesSent();

    /**
     * Get bytes received from remote sources
     *
     * @return Bytes received
     */
    long getBytesReceived();

    /**
     * Get duration of ProcessSession operations in nanoseconds
     *
     * @return Processing duration
     */
    long getProcessingNanoseconds();

    /**
     * Get CPU duration of operations in nanoseconds
     *
     * @return CPU duration
     */
    long getCpuNanoseconds();

    /**
     * Get content read duration in nanoseconds
     *
     * @return Content read duration
     */
    long getContentReadNanoseconds();

    /**
     * Get content write duration in nanoseconds
     *
     * @return Content write duration
     */
    long getContentWriteNanoseconds();

    /**
     * Get ProcessSession commit duration in nanoseconds
     *
     * @return Session commit duration
     */
    long getSessionCommitNanoseconds();

    /**
     * Get Garbage Collection duration in milliseconds
     *
     * @return Garbage Collection duration
     */
    long getGarbageCollectionMillis();

    /**
     * Get aggregate FlowFile lineage duration in milliseconds
     *
     * @return Aggregative lineage duration
     */
    long getAggregateLineageMillis();

    /**
     * Map of Counter name to Counter value number
     *
     * @return Map of Counters
     */
    Map<String, Long> getCounters();
}
