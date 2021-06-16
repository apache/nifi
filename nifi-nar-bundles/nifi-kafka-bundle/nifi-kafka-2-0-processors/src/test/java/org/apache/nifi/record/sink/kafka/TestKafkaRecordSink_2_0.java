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
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils;
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
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestKafkaRecordSink_2_0 {

    private static final String TOPIC_NAME = "unit-test";

    @Test
    public void testRecordFormat() throws IOException, InitializationException {
        MockKafkaRecordSink_2_0 task = initTask();

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        );
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", "Hello");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("field1", 6);
        row2.put("field2", "World!");

        RecordSet recordSet = new ListRecordSet(recordSchema, Arrays.asList(
                new MapRecord(recordSchema, row1),
                new MapRecord(recordSchema, row2)
        ));

        task.sendData(recordSet, new HashMap<>(), true);

        assertEquals(1, task.dataSent.size());
        String[] lines = new String(task.dataSent.get(0)).split("\n");
        assertNotNull(lines);
        assertEquals(2, lines.length);
        String[] data = lines[0].split(",");
        assertEquals("15", data[0]); // In the MockRecordWriter all values are strings
        assertEquals("Hello", data[1]);
        data = lines[1].split(",");
        assertEquals("6", data[0]);
        assertEquals("World!", data[1]);
    }

    private MockKafkaRecordSink_2_0 initTask() throws InitializationException {

        final ComponentLog logger = mock(ComponentLog.class);
        final MockKafkaRecordSink_2_0 task = new MockKafkaRecordSink_2_0();
        ConfigurationContext context = mock(ConfigurationContext.class);
        final StateManager stateManager = new MockStateManager(task);

        final PropertyValue topicValue = Mockito.mock(StandardPropertyValue.class);
        when(topicValue.evaluateAttributeExpressions()).thenReturn(topicValue);
        when(topicValue.getValue()).thenReturn(TOPIC_NAME);
        when(context.getProperty(KafkaRecordSink_2_0.TOPIC)).thenReturn(topicValue);

        final PropertyValue deliveryValue = Mockito.mock(StandardPropertyValue.class);
        when(deliveryValue.getValue()).thenReturn(KafkaRecordSink_2_0.DELIVERY_REPLICATED.getValue());
        when(context.getProperty(KafkaRecordSink_2_0.DELIVERY_GUARANTEE)).thenReturn(deliveryValue);

        final PropertyValue maxSizeValue = Mockito.mock(StandardPropertyValue.class);
        when(maxSizeValue.asDataSize(DataUnit.B)).thenReturn(1024.0);
        when(context.getProperty(KafkaRecordSink_2_0.MAX_REQUEST_SIZE)).thenReturn(maxSizeValue);

        final PropertyValue maxAckWaitValue = Mockito.mock(StandardPropertyValue.class);
        when(maxAckWaitValue.asTimePeriod(TimeUnit.MILLISECONDS)).thenReturn(5000L);
        when(context.getProperty(KafkaRecordSink_2_0.ACK_WAIT_TIME)).thenReturn(maxAckWaitValue);

        final PropertyValue charEncodingValue = Mockito.mock(StandardPropertyValue.class);
        when(charEncodingValue.evaluateAttributeExpressions()).thenReturn(charEncodingValue);
        when(charEncodingValue.getValue()).thenReturn("UTF-8");
        when(context.getProperty(KafkaRecordSink_2_0.MESSAGE_HEADER_ENCODING)).thenReturn(charEncodingValue);

        final PropertyValue securityValue = Mockito.mock(StandardPropertyValue.class);
        when(securityValue.getValue()).thenReturn(KafkaProcessorUtils.SEC_SASL_PLAINTEXT.getValue());
        when(context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL)).thenReturn(securityValue);

        final PropertyValue jaasValue = Mockito.mock(StandardPropertyValue.class);
        when(jaasValue.evaluateAttributeExpressions()).thenReturn(jaasValue);
        when(jaasValue.getValue()).thenReturn(null);
        when(context.getProperty(KafkaProcessorUtils.JAAS_SERVICE_NAME)).thenReturn(jaasValue);

        Map<PropertyDescriptor, String> propertyMap = new HashMap<>();
        propertyMap.put(KafkaRecordSink_2_0.TOPIC, KafkaRecordSink_2_0.TOPIC.getName());
        propertyMap.put(KafkaRecordSink_2_0.DELIVERY_GUARANTEE, KafkaRecordSink_2_0.DELIVERY_GUARANTEE.getName());
        propertyMap.put(KafkaRecordSink_2_0.MAX_REQUEST_SIZE, KafkaRecordSink_2_0.MAX_REQUEST_SIZE.getName());
        propertyMap.put(KafkaRecordSink_2_0.ACK_WAIT_TIME, KafkaRecordSink_2_0.ACK_WAIT_TIME.getName());
        propertyMap.put(KafkaRecordSink_2_0.MESSAGE_HEADER_ENCODING, KafkaRecordSink_2_0.MESSAGE_HEADER_ENCODING.getName());

        when(context.getProperties()).thenReturn(propertyMap);

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        // No header, don't quote values
        MockRecordWriter writer = new MockRecordWriter(null, false);
        when(context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY)).thenReturn(pValue);
        when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer);
        when(context.getProperty(KafkaProcessorUtils.SSL_CONTEXT_SERVICE)).thenReturn(pValue);
        when(pValue.asControllerService(SSLContextService.class)).thenReturn(null);
        when(context.getProperty(KafkaProcessorUtils.KERBEROS_CREDENTIALS_SERVICE)).thenReturn(pValue);
        when(pValue.asControllerService(KerberosCredentialsService.class)).thenReturn(null);

        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(task, UUID.randomUUID().toString(), logger, stateManager);
        task.initialize(initContext);
        task.onEnabled(context);
        return task;
    }

    private static class MockKafkaRecordSink_2_0 extends KafkaRecordSink_2_0 {
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
                        return new FutureTask(() -> {}, recordMetadata);
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