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
package org.apache.nifi.kafka.shared.attribute;

/**
 * Kafka FlowFile Attributes
 */
public interface KafkaFlowFileAttribute {
    String KAFKA_KEY = "kafka.key";

    String KAFKA_TOPIC = "kafka.topic";

    String KAFKA_PARTITION = "kafka.partition";

    String KAFKA_OFFSET = "kafka.offset";

    String KAFKA_MAX_OFFSET = "kafka.max.offset";

    String KAFKA_LEADER_EPOCH = "kafka.leader.epoch";

    String KAFKA_TIMESTAMP = "kafka.timestamp";

    String KAFKA_COUNT = "kafka.count";

    String KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.id";

    String KAFKA_CONSUMER_OFFSETS_COMMITTED = "kafka.consumer.offsets.committed";

    String KAFKA_TOMBSTONE = "kafka.tombstone";

    String KAFKA_HEADER_COUNT = "kafka.header.count";
}
