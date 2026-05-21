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
package org.apache.nifi.kafka.processors.consumer;

import org.apache.nifi.components.DescribedValue;

/**
 * Selects the Kafka consumer group model used by ConsumeKafka.
 */
public enum GroupType implements DescribedValue {
    CONSUMER(
            "Consumer Group",
            "Use a classic Kafka consumer group. Records are partition-bound: each partition is assigned to one consumer in the group at a time, "
                    + "offset positioning is committed per partition, and parallelism is capped by the number of partitions in the subscribed topics."
    ),

    SHARE(
            "Share Group",
            "Use a Kafka share group (KIP-932). Records are distributed cooperatively across the consumers of the share group with per-record acknowledgement, "
                    + "and parallelism is no longer capped by the number of partitions. Requires Kafka 4.1+ brokers configured for share groups."
    );

    private final String displayName;
    private final String description;

    GroupType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
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
