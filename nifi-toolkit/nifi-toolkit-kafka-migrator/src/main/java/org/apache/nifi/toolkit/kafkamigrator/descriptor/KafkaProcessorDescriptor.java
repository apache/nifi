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
package org.apache.nifi.toolkit.kafkamigrator.descriptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaProcessorDescriptor implements ProcessorDescriptor {
    private static final Map<String, String> CONSUME_KAFKA_PROCESSOR_PROPERTIES;
    private static final Map<String, String> CONSUME_PROPERTIES_TO_BE_SAVED;
    private static final Map<String, String> PUBLISH_KAFKA_PROCESSOR_PROPERTIES;
    private static final Map<String, String> PUBLISH_PROPERTIES_TO_BE_SAVED;
    private static final Map<String, String> CONTROLLER_SERVICES;
    private static final Map<KafkaProcessorType, Map<String, String>> PROPERTIES;
    private static final Map<KafkaProcessorType, Map<String, String>> PROPERTIES_TO_BE_SAVED;

    static {
        CONSUME_KAFKA_PROCESSOR_PROPERTIES = new HashMap<>();
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("security.protocol", "PLAINTEXT");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.mechanism", "GSSAPI");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.kerberos.service.name", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("kerberos-credentials-service", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.kerberos.principal", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.kerberos.keytab", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.username", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.password", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("sasl.token.auth", "false");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("ssl.context.service", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("topic", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("topic_type", "names");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("group.id", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("auto.offset.reset", "latest");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("key-attribute-encoding", "utf-8");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("message-demarcator", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("separate-by-key", "false");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("message-header-encoding", "UTF-8");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("header-name-regex", null);
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("max.poll.records", "10000");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("max-uncommit-offset-wait", "1 secs");
        CONSUME_KAFKA_PROCESSOR_PROPERTIES.put("Communications Timeout", "60 secs");

        CONSUME_PROPERTIES_TO_BE_SAVED = new HashMap<>();
        CONSUME_PROPERTIES_TO_BE_SAVED.put("Topic Name", "topic");
        CONSUME_PROPERTIES_TO_BE_SAVED.put("Group ID", "group.id");

        PUBLISH_KAFKA_PROCESSOR_PROPERTIES = new HashMap<>();
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("security.protocol", "PLAINTEXT");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.mechanism", "GSSAPI");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.kerberos.service.name", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("kerberos-credentials-service", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.kerberos.principal", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.kerberos.keytab", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.username", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.password", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("sasl.token.auth", "false");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("ssl.context.service", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("topic", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("acks", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("Failure Strategy", "Route to Failure");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("transactional-id-prefix", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("attribute-name-regex", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("message-header-encoding", "UTF-8");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("kafka-key", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("key-attribute-encoding", "utf-8");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("message-demarcator", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("max.request.size", "1 MB");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("ack.wait.time", "5 secs");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("max.block.ms", "5 sec");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("partition", null);
        PUBLISH_KAFKA_PROCESSOR_PROPERTIES.put("compression.type", null);

        PUBLISH_PROPERTIES_TO_BE_SAVED = new HashMap<>();
        PUBLISH_PROPERTIES_TO_BE_SAVED.put("Topic Name", "topic");
        PUBLISH_PROPERTIES_TO_BE_SAVED.put("Partition", "partition");
        PUBLISH_PROPERTIES_TO_BE_SAVED.put("Kafka Key", "kafka-key");
        PUBLISH_PROPERTIES_TO_BE_SAVED.put("Delivery Guarantee", "acks");
        PUBLISH_PROPERTIES_TO_BE_SAVED.put( "Compression Codec", "compression.type");

        CONTROLLER_SERVICES = new HashMap<>();
        CONTROLLER_SERVICES.put("kerberos-credentials-service", "org.apache.nifi.kerberos.KerberosCredentialsService");
        CONTROLLER_SERVICES.put("ssl.context.service", "org.apache.nifi.ssl.SSLContextService");

        PROPERTIES = new HashMap<>();
        PROPERTIES.put(KafkaProcessorType.CONSUME, CONSUME_KAFKA_PROCESSOR_PROPERTIES);
        PROPERTIES.put(KafkaProcessorType.PUBLISH, PUBLISH_KAFKA_PROCESSOR_PROPERTIES);

        PROPERTIES_TO_BE_SAVED = new HashMap<>();
        PROPERTIES_TO_BE_SAVED.put(KafkaProcessorType.CONSUME, CONSUME_PROPERTIES_TO_BE_SAVED);
        PROPERTIES_TO_BE_SAVED.put(KafkaProcessorType.PUBLISH, PUBLISH_PROPERTIES_TO_BE_SAVED);
    }

    private final KafkaProcessorType processorType;

    public KafkaProcessorDescriptor(final KafkaProcessorType processorType) {
        this.processorType = processorType;
    }

    @Override
    public Map<String, String> getProcessorProperties() {
        return Collections.unmodifiableMap(PROPERTIES.get(processorType));
    }

    @Override
    public Map<String, String> getPropertiesToBeSaved() {
        return Collections.unmodifiableMap(PROPERTIES_TO_BE_SAVED.get(processorType));
    }

    @Override
    public Map<String, String> getControllerServicesForTemplates() {
        return Collections.unmodifiableMap(CONTROLLER_SERVICES);
    }
}
