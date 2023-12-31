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
package org.apache.nifi.record.sink.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestKafkaRecordSink_2_6 {

    private static final String TOPIC_NAME = "unit-test";

    @Test
    public void testRecordFormat() throws IOException, InitializationException {
        MockKafkaRecordSink_2_6 task = initTask();

        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        ));

        MapRecord row1 = new MapRecord(recordSchema, Map.of(
        "field1", 15,
        "field2", "Hello"
        ));
        MapRecord row2 = new MapRecord(recordSchema, Map.of(
        "field1", 6,
        "field2", "World!"
        ));

        RecordSet recordSet = new ListRecordSet(recordSchema, List.of(row1, row2));

        task.sendData(recordSet, new HashMap<>(), true);

        assertEquals(2, task.dataSent.size());
        String[] lines = new String(task.dataSent.getFirst()).split("\n");
        assertNotNull(lines);
        assertEquals(1, lines.length);
        String[] data = lines[0].split(",");
        assertEquals("15", data[0]); // In the MockRecordWriter all values are strings
        assertEquals("Hello", data[1]);
        lines = new String(task.dataSent.get(1)).split("\n");
        assertNotNull(lines);
        assertEquals(1, lines.length);
        data = lines[0].split(",");
        assertEquals("6", data[0]);
        assertEquals("World!", data[1]);
    }

    private MockKafkaRecordSink_2_6 initTask() throws InitializationException {

        final ComponentLog logger = mock(ComponentLog.class);
        final MockKafkaRecordSink_2_6 task = new MockKafkaRecordSink_2_6();
        ConfigurationContext context = mock(ConfigurationContext.class);
        final StateManager stateManager = new MockStateManager(task);

        final PropertyValue topicValue = Mockito.mock(StandardPropertyValue.class);
        when(topicValue.evaluateAttributeExpressions()).thenReturn(topicValue);
        when(topicValue.getValue()).thenReturn(TOPIC_NAME);
        when(context.getProperty(KafkaRecordSink_2_6.TOPIC)).thenReturn(topicValue);

        final PropertyValue deliveryValue = Mockito.mock(StandardPropertyValue.class);
        when(deliveryValue.getValue()).thenReturn(KafkaRecordSink_2_6.DELIVERY_REPLICATED.getValue());
        when(context.getProperty(KafkaRecordSink_2_6.DELIVERY_GUARANTEE)).thenReturn(deliveryValue);

        final PropertyValue maxSizeValue = Mockito.mock(StandardPropertyValue.class);
        when(maxSizeValue.asDataSize(DataUnit.B)).thenReturn(1024.0);
        when(context.getProperty(KafkaRecordSink_2_6.MAX_REQUEST_SIZE)).thenReturn(maxSizeValue);

        final PropertyValue maxAckWaitValue = Mockito.mock(StandardPropertyValue.class);
        when(maxAckWaitValue.asTimePeriod(TimeUnit.MILLISECONDS)).thenReturn(5000L);
        when(context.getProperty(KafkaRecordSink_2_6.ACK_WAIT_TIME)).thenReturn(maxAckWaitValue);

        final PropertyValue charEncodingValue = Mockito.mock(StandardPropertyValue.class);
        when(charEncodingValue.evaluateAttributeExpressions()).thenReturn(charEncodingValue);
        when(charEncodingValue.getValue()).thenReturn("UTF-8");
        when(context.getProperty(KafkaRecordSink_2_6.MESSAGE_HEADER_ENCODING)).thenReturn(charEncodingValue);

        final PropertyValue securityValue = Mockito.mock(StandardPropertyValue.class);
        when(securityValue.getValue()).thenReturn(SecurityProtocol.PLAINTEXT.name());
        when(context.getProperty(KafkaRecordSink_2_6.SECURITY_PROTOCOL)).thenReturn(securityValue);

        final PropertyValue jaasValue = Mockito.mock(StandardPropertyValue.class);
        when(jaasValue.evaluateAttributeExpressions()).thenReturn(jaasValue);
        when(jaasValue.getValue()).thenReturn(null);
        when(context.getProperty(KafkaRecordSink_2_6.KERBEROS_SERVICE_NAME)).thenReturn(jaasValue);

        Map<PropertyDescriptor, String> propertyMap = new HashMap<>();
        propertyMap.put(KafkaRecordSink_2_6.TOPIC, KafkaRecordSink_2_6.TOPIC.getName());
        propertyMap.put(KafkaRecordSink_2_6.DELIVERY_GUARANTEE, KafkaRecordSink_2_6.DELIVERY_GUARANTEE.getName());
        propertyMap.put(KafkaRecordSink_2_6.MAX_REQUEST_SIZE, KafkaRecordSink_2_6.MAX_REQUEST_SIZE.getName());
        propertyMap.put(KafkaRecordSink_2_6.ACK_WAIT_TIME, KafkaRecordSink_2_6.ACK_WAIT_TIME.getName());
        propertyMap.put(KafkaRecordSink_2_6.MESSAGE_HEADER_ENCODING, KafkaRecordSink_2_6.MESSAGE_HEADER_ENCODING.getName());

        when(context.getProperties()).thenReturn(propertyMap);

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        // No header, don't quote values
        MockRecordWriter writer = new MockRecordWriter(null, false);
        when(context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY)).thenReturn(pValue);
        when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer);
        when(context.getProperty(KafkaRecordSink_2_6.SSL_CONTEXT_SERVICE)).thenReturn(pValue);
        when(pValue.asControllerService(SSLContextService.class)).thenReturn(null);

        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(task, UUID.randomUUID().toString(), logger, stateManager);
        task.initialize(initContext);
        task.onEnabled(context);
        return task;
    }

    private static class MockKafkaRecordSink_2_6 extends KafkaRecordSink_2_6 {
        final List<byte[]> dataSent = new ArrayList<>();

        @SuppressWarnings("unchecked")
        @Override
        protected Producer<byte[], byte[]> createProducer(Map<String, Object> kafkaProperties) {
            final Producer<byte[], byte[]> mockProducer = (Producer<byte[], byte[]>) mock(Producer.class);
            when(mockProducer.send(Mockito.argThat(new ByteProducerRecordMatcher()), any(Callback.class))).then(
                    (Answer<Future<RecordMetadata>>) invocationOnMock -> {
                        ProducerRecord<byte[], byte[]> producerRecord = invocationOnMock.getArgument(0);
                        final byte[] data = producerRecord.value();
                        dataSent.add(data);
                        Callback callback = invocationOnMock.getArgument(1);
                        RecordMetadata recordMetadata = new RecordMetadata(
                                new TopicPartition(producerRecord.topic(), producerRecord.partition() != null ? producerRecord.partition() : 0),
                                0,
                                data.length,
                                producerRecord.timestamp() != null ? producerRecord.timestamp() : System.currentTimeMillis(),
                                0L,
                                producerRecord.key() != null ? producerRecord.key().length : 0,
                                data.length);
                        callback.onCompletion(recordMetadata, null);
                        return new FutureTask<>(() -> {}, recordMetadata);
                    });
            return mockProducer;
        }
    }

    private static class ByteProducerRecordMatcher implements ArgumentMatcher<ProducerRecord<byte[], byte[]>> {

        @Override
        public boolean matches(ProducerRecord<byte[], byte[]> producer) {
            return true;
        }
    }
}