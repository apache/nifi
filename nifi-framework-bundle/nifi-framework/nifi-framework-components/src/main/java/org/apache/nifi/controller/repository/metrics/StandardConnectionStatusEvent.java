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
package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ConnectionStatusEvent;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.controller.status.LoadBalanceStatus;

/**
 * Standard record representation of Connection Status Event with package-private visibility for Builder
 */
record StandardConnectionStatusEvent(
        ComponentMetricContext componentMetricContext,
        long backPressureBytesThreshold,
        long backPressureObjectThreshold,
        long queuedBytes,
        int queuedCount,
        LoadBalanceStatus loadBalanceStatus,
        FlowFileAvailability flowFileAvailability
) implements ConnectionStatusEvent {

    @Override
    public ComponentMetricContext getComponentMetricContext() {
        return componentMetricContext;
    }

    @Override
    public long getBackPressureBytesThreshold() {
        return backPressureBytesThreshold;
    }

    @Override
    public long getBackPressureObjectThreshold() {
        return backPressureObjectThreshold;
    }

    @Override
    public long getQueuedBytes() {
        return queuedBytes;
    }

    @Override
    public int getQueuedCount() {
        return queuedCount;
    }

    @Override
    public LoadBalanceStatus getLoadBalanceStatus() {
        return loadBalanceStatus;
    }

    @Override
    public FlowFileAvailability getFlowFileAvailability() {
        return flowFileAvailability;
    }
}
