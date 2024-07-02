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
package org.apache.nifi.kafka.processors.producer;

import org.apache.nifi.components.DescribedValue;

public enum PartitionStrategy implements DescribedValue {

    ROUND_ROBIN_PARTITIONING(
            "org.apache.nifi.processors.kafka.pubsub.Partitioners.RoundRobinPartitioner",
            "RoundRobinPartitioner",
            "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
                    + "the next Partition to Partition 2, and so on, wrapping as necessary."),
    RANDOM_PARTITIONING(
            "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
            "DefaultPartitioner",
            "The default partitioning strategy will choose the sticky partition that changes when the batch is full "
                    + "(See KIP-480 for details about sticky partitioning)."),
    EXPRESSION_LANGUAGE_PARTITIONING(
            "org.apache.nifi.processors.kafka.pubsub.Partitioners.ExpressionLanguagePartitioner",
            "Expression Language Partitioner",
            "Interprets the <Partition> property as Expression Language that will be evaluated against each FlowFile. This Expression will be evaluated once against the FlowFile, " +
                    "so all Records in a given FlowFile will go to the same partition.");

    private final String value;
    private final String displayName;
    private final String description;

    PartitionStrategy(final String value, final String displayName, final String description) {
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
