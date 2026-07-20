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

import java.util.Objects;

/**
 * Builder for Connection Status Events with Component Metric Context required
 */
public class ConnectionStatusEventBuilder {

    private final ComponentMetricContext componentMetricContext;

    private long backPressureBytesThreshold;
    private long backPressureObjectThreshold;
    private long queuedBytes;
    private int queuedCount;
    private LoadBalanceStatus loadBalanceStatus = LoadBalanceStatus.LOAD_BALANCE_NOT_CONFIGURED;
    private FlowFileAvailability flowFileAvailability = FlowFileAvailability.ACTIVE_QUEUE_EMPTY;

    private ConnectionStatusEventBuilder(final ComponentMetricContext componentMetricContext) {
        this.componentMetricContext = Objects.requireNonNull(componentMetricContext, "Component Metric Context required");
    }

    public static ConnectionStatusEventBuilder forComponent(final ComponentMetricContext componentMetricContext) {
        return new ConnectionStatusEventBuilder(componentMetricContext);
    }

    public ConnectionStatusEventBuilder backPressureBytesThreshold(final long backPressureBytesThreshold) {
        this.backPressureBytesThreshold = backPressureBytesThreshold;
        return this;
    }

    public ConnectionStatusEventBuilder backPressureObjectThreshold(final long backPressureObjectThreshold) {
        this.backPressureObjectThreshold = backPressureObjectThreshold;
        return this;
    }

    public ConnectionStatusEventBuilder queuedBytes(final long queuedBytes) {
        this.queuedBytes = queuedBytes;
        return this;
    }

    public ConnectionStatusEventBuilder queuedCount(final int queuedCount) {
        this.queuedCount = queuedCount;
        return this;
    }

    public ConnectionStatusEventBuilder loadBalanceStatus(final LoadBalanceStatus loadBalanceStatus) {
        this.loadBalanceStatus = Objects.requireNonNull(loadBalanceStatus, "Load Balance Status required");
        return this;
    }

    public ConnectionStatusEventBuilder flowFileAvailability(final FlowFileAvailability flowFileAvailability) {
        this.flowFileAvailability = Objects.requireNonNull(flowFileAvailability, "FlowFile Availability required");
        return this;
    }

    public ConnectionStatusEvent build() {
        return new StandardConnectionStatusEvent(
                componentMetricContext,
                backPressureBytesThreshold,
                backPressureObjectThreshold,
                queuedBytes,
                queuedCount,
                loadBalanceStatus,
                flowFileAvailability
        );
    }
}
