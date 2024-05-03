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
package org.apache.nifi.kafka.shared.component;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.kafka.shared.property.FailureStrategy;

/**
 * Kafka Publish Component interface with common Property Descriptors
 */
public interface KafkaPublishComponent extends KafkaClientComponent {
    PropertyDescriptor FAILURE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Failure Strategy")
            .displayName("Failure Strategy")
            .description("Specifies how the processor handles a FlowFile if it is unable to publish the data to Kafka")
            .required(true)
            .allowableValues(FailureStrategy.class)
            .defaultValue(FailureStrategy.ROUTE_TO_FAILURE)
            .build();
}
