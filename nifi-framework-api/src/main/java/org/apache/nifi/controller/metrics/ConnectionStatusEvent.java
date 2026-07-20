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

import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.controller.status.LoadBalanceStatus;

/**
 * Event abstraction for Connection Status metrics collected during ProcessSession operations
 */
public interface ConnectionStatusEvent {
    /**
     * Get Component Metric Context describing the Component associated with the recorded ProcessSession operations
     *
     * @return Component Metric Context
     */
    ComponentMetricContext getComponentMetricContext();

    /**
     * Get configured Back Pressure Bytes Threshold
     *
     * @return Back Pressure Bytes Threshold
     */
    long getBackPressureBytesThreshold();

    /**
     * Get configured Back Pressure Object Threshold
     *
     * @return Back Pressure Object Threshold
     */
    long getBackPressureObjectThreshold();

    /**
     * Get bytes from queued FlowFiles for the Connection
     *
     * @return Queued Bytes
     */
    long getQueuedBytes();

    /**
     * Get count of FlowFiles queued for the Connection
     *
     * @return Queued FlowFiles
     */
    int getQueuedCount();

    /**
     * Get Load Balance Status for the Connection
     *
     * @return Load Balance Status
     */
    LoadBalanceStatus getLoadBalanceStatus();

    /**
     * Get FlowFile Availability for the Connection
     *
     * @return FlowFile Availability
     */
    FlowFileAvailability getFlowFileAvailability();
}
