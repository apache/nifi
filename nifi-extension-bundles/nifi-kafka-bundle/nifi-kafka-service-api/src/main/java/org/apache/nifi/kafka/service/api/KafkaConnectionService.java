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
package org.apache.nifi.kafka.service.api;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.consumer.share.KafkaShareConsumerService;
import org.apache.nifi.kafka.service.api.consumer.share.ShareGroupContext;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;

public interface KafkaConnectionService extends ControllerService {

    KafkaConsumerService getConsumerService(PollingContext pollingContext);

    KafkaProducerService getProducerService(ProducerConfiguration producerConfiguration);

    String getBrokerUri();

    /**
     * Build a Kafka share-group consumer for the given subscription.
     * Implementations that do not support share groups (KIP-932) may throw
     * {@link UnsupportedOperationException}; the default implementation does so to preserve
     * backward compatibility for existing connection services.
     *
     * @param shareGroupContext Subscription context describing the share group, topics, and
     *                          acknowledgement mode
     * @return Share-group consumer service
     */
    default KafkaShareConsumerService getShareConsumerService(ShareGroupContext shareGroupContext) {
        throw new UnsupportedOperationException("Share consumer not supported by this Kafka Connection Service");
    }
}
