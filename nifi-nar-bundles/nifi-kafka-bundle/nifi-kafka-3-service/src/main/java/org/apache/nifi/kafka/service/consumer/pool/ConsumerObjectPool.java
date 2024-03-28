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
package org.apache.nifi.kafka.service.consumer.pool;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.Properties;

/**
 * Kafka Consumer Object Pool
 */
public class ConsumerObjectPool extends GenericKeyedObjectPool<Subscription, Consumer<byte[], byte[]>> {
    /**
     * Consumer Object Pool constructor with Kafka Consumer Properties
     *
     * @param consumerProperties Kafka Consumer Properties required
     */
    public ConsumerObjectPool(final Properties consumerProperties) {
        super(new ConsumerPooledObjectFactory(consumerProperties, new StandardConsumerFactory()));
    }
}
