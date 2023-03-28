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
package org.apache.nifi.kafka.processors.producer.config;

import org.apache.nifi.components.DescribedValue;

public enum DeliveryGuarantee implements DescribedValue {

    DELIVERY_REPLICATED("all", "Guarantee Replicated Delivery",
            "FlowFile will be routed to failure unless the message is replicated to the appropriate "
                    + "number of Kafka Nodes according to the Topic configuration"),
    DELIVERY_ONE_NODE("1", "Guarantee Single Node Delivery",
            "FlowFile will be routed to success if the message is received by a single Kafka node, "
                    + "whether or not it is replicated. This is faster than <Guarantee Replicated Delivery> "
                    + "but can result in data loss if a Kafka node crashes"),
    DELIVERY_BEST_EFFORT("0", "Best Effort",
            "FlowFile will be routed to success after successfully sending the content to a Kafka node, "
                    + "without waiting for any acknowledgment from the node at all. This provides the best performance but may result in data loss.");

    private final String value;
    private final String displayName;
    private final String description;

    DeliveryGuarantee(final String value, final String displayName, final String description) {
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
