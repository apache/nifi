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
package org.apache.nifi.kafka.shared.property;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supported Kafka Publishing Failure Strategies
 */
public enum FailureStrategy implements DescribedValue {
    ROUTE_TO_FAILURE("Route to Failure", "Route to Failure", "When unable to publish records to Kafka, the FlowFile will be routed to the failure relationship."),
    ROLLBACK("Rollback", "Rollback", "When unable to publish records to Kafka, the FlowFile will be placed back on the queue so that it will be retried. " +
            "For flows where FlowFile ordering is important, this strategy can be used along with ensuring that the each processor uses only a single Concurrent Task.");

    private final String value;
    private final String displayName;
    private final String description;

    FailureStrategy(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
