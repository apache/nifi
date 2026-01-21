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

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class KafkaTopicsStep {
    public static final String STEP_NAME = "Kafka Topics";

    public static final ConnectorPropertyDescriptor TOPIC_NAMES = new ConnectorPropertyDescriptor.Builder()
        .name("Topic Names")
        .description("A comma-separated list of Kafka topics to consume from.")
        .required(true)
        .type(PropertyType.STRING_LIST)
        .allowableValuesFetchable(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor CONSUMER_GROUP_ID = new ConnectorPropertyDescriptor.Builder()
        .name("Consumer Group ID")
        .description("The consumer group ID to use when connecting to Kafka.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor OFFSET_RESET = new ConnectorPropertyDescriptor.Builder()
        .name("Offset Reset")
        .description("What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server.")
        .required(true)
        .type(PropertyType.STRING)
        .defaultValue("latest")
        .allowableValues("earliest", "latest", "none")
        .build();

    public static final ConnectorPropertyDescriptor KAFKA_DATA_FORMAT = new ConnectorPropertyDescriptor.Builder()
        .name("Kafka Data Format")
        .description("The format of the data in Kafka topics.")
        .required(true)
        .type(PropertyType.STRING)
        .defaultValue("Avro")
        .allowableValues("Avro", "JSON")
        .build();

    public static final ConnectorPropertyGroup KAFKA_TOPICS_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Topics Configuration")
        .description("Properties for configuring Kafka topics consumption.")
        .properties(List.of(
            TOPIC_NAMES,
            CONSUMER_GROUP_ID,
            OFFSET_RESET,
            KAFKA_DATA_FORMAT
        ))
        .build();

    public static final ConfigurationStep KAFKA_TOPICS_STEP = new ConfigurationStep.Builder()
        .name(STEP_NAME)
        .description("Kafka topics to consume from.")
        .propertyGroups(List.of(
            KAFKA_TOPICS_GROUP
        ))
        .build();

}
