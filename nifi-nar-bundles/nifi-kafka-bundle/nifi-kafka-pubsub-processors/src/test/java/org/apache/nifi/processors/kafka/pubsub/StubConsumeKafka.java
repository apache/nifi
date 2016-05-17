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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class StubConsumeKafka extends ConsumeKafka {

    private byte[][] values;

    public void setValues(byte[][] values) {
        this.values = values;
    }


    @SuppressWarnings("unchecked")
    @Override
    protected Consumer<byte[], byte[]> buildKafkaResource(ProcessContext context, ProcessSession session) {
        Consumer<byte[], byte[]> consumer = super.buildKafkaResource(context, session);
        consumer = mock(Consumer.class);
        String topicName = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();

        when(consumer.poll(Mockito.anyLong())).thenAnswer(new Answer<ConsumerRecords<byte[], byte[]>>() {
            @Override
            public ConsumerRecords<byte[], byte[]> answer(InvocationOnMock invocation) throws Throwable {
                List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
                for (int i = 0; i < StubConsumeKafka.this.values.length; i++) {
                    byte[] value = StubConsumeKafka.this.values[i];
                    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(topicName, 0, 0, null, value);
                    records.add(record);
                }
                TopicPartition partition = new TopicPartition(topicName, 0);
                Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> m = new LinkedHashMap<>();
                m.put(partition, records);
                return new ConsumerRecords<>(m);
            }
        });

        return consumer;
    }
}
