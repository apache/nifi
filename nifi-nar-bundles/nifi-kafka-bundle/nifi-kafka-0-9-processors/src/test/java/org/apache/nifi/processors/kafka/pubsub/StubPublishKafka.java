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
package org.apache.nifi.processors.kafka.pubsub;

import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.BOOTSTRAP_SERVERS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class StubPublishKafka extends PublishKafka {

    private volatile Producer<byte[], byte[]> producer;

    private volatile boolean failed;

    private final int ackCheckSize;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Map<Object, Object> msgsSent = new ConcurrentHashMap<>();

    StubPublishKafka(int ackCheckSize) {
        this.ackCheckSize = ackCheckSize;
    }

    public Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    public void destroy() {
        this.executor.shutdownNow();
    }

    public Map<Object, Object> getMessagesSent() {
        return new HashMap<>(msgsSent);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected KafkaPublisher buildKafkaResource(ProcessContext context, ProcessSession session)
            throws ProcessException {
        final Map<String, String> kafkaProperties = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ProducerConfig.class, kafkaProperties);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaPublisher publisher;
        try {
            Field f = PublishKafka.class.getDeclaredField("brokers");
            f.setAccessible(true);
            f.set(this, context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue());
            publisher = (KafkaPublisher) TestUtils.getUnsafe().allocateInstance(KafkaPublisher.class);
            publisher.setAckWaitTime(15000);
            producer = mock(Producer.class);

            this.instrumentProducer(producer, false);
            Field kf = KafkaPublisher.class.getDeclaredField("kafkaProducer");
            kf.setAccessible(true);
            kf.set(publisher, producer);

            Field componentLogF = KafkaPublisher.class.getDeclaredField("componentLog");
            componentLogF.setAccessible(true);
            componentLogF.set(publisher, mock(ComponentLog.class));

            Field ackCheckSizeField = KafkaPublisher.class.getDeclaredField("ackCheckSize");
            ackCheckSizeField.setAccessible(true);
            ackCheckSizeField.set(publisher, this.ackCheckSize);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
        return publisher;
    }

    @SuppressWarnings("unchecked")
    private void instrumentProducer(Producer<byte[], byte[]> producer, boolean failRandomly) {

        when(producer.send(Mockito.any(ProducerRecord.class))).then(new Answer<Future<RecordMetadata>>() {
            @Override
            public Future<RecordMetadata> answer(InvocationOnMock invocation) throws Throwable {
                final ProducerRecord<byte[], byte[]> record = invocation.getArgumentAt(0, ProducerRecord.class);
                if (record != null && record.key() != null) {
                    msgsSent.put(record.key(), record.value());
                }

                String value = new String(record.value(), StandardCharsets.UTF_8);
                if ("fail".equals(value) && !StubPublishKafka.this.failed) {
                    StubPublishKafka.this.failed = true;
                    throw new RuntimeException("intentional");
                }
                Future<RecordMetadata> future = executor.submit(new Callable<RecordMetadata>() {
                    @Override
                    public RecordMetadata call() throws Exception {
                        if ("futurefail".equals(value) && !StubPublishKafka.this.failed) {
                            StubPublishKafka.this.failed = true;
                            throw new TopicAuthorizationException("Unauthorized");
                        } else {
                            TopicPartition partition = new TopicPartition("foo", 0);
                            RecordMetadata meta = new RecordMetadata(partition, 0, 0);
                            return meta;
                        }
                    }
                });
                return future;
            }
        });
    }
}
