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

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConsumerPooledObjectFactoryTest {
    private static final String GROUP_ID = ConsumerPooledObjectFactoryTest.class.getSimpleName();

    private static final String TOPIC = String.class.getSimpleName();

    @Mock
    ConsumerFactory consumerFactory;

    @Mock
    Consumer<byte[], byte[]> mockConsumer;

    ConsumerPooledObjectFactory factory;

    @BeforeEach
    void setFactory() {
        final Properties properties = new Properties();
        factory = new ConsumerPooledObjectFactory(properties, consumerFactory);
    }

    @Test
    void testCreate() {
        final Collection<String> topics = Collections.singleton(TOPIC);
        final Subscription subscription = new Subscription(GROUP_ID, topics, AutoOffsetReset.EARLIEST);

        when(consumerFactory.newConsumer(any(Properties.class))).thenReturn(mockConsumer);
        final Consumer<byte[], byte[]> consumer = factory.create(subscription);

        assertNotNull(consumer);

        verify(mockConsumer).subscribe(anyCollection());
    }

    @Test
    void testWrap() {
        final PooledObject<Consumer<byte[], byte[]>> pooledObject = factory.wrap(mockConsumer);

        assertNotNull(pooledObject);
    }

    @Test
    void testDestroyObject() {
        final PooledObject<Consumer<byte[], byte[]>> pooledObject = new DefaultPooledObject<>(mockConsumer);

        final Subscription subscription = new Subscription(GROUP_ID, Collections.singleton(TOPIC), AutoOffsetReset.EARLIEST);
        factory.destroyObject(subscription, pooledObject);

        verify(mockConsumer).close();
    }
}
