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
package org.apache.nifi.amqp.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.apache.nifi.amqp.processors.ConsumeAMQP.OutputHeaderFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ConsumeAMQPTest {

    @Test
    public void testMessageAcked() throws TimeoutException, IOException {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Collections.singletonList("queue1"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");
            sender.publish("world".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.AUTO_ACKNOWLEDGE, "false");

            runner.run();

            runner.assertTransferCount(ConsumeAMQP.REL_SUCCESS, 2);

            final MockFlowFile helloFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            helloFF.assertContentEquals("hello");

            final MockFlowFile worldFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(1);
            worldFF.assertContentEquals("world");

            // A single cumulative ack should be used
            assertFalse(((TestChannel) connection.createChannel()).isAck(0));
            assertTrue(((TestChannel) connection.createChannel()).isAck(1));
        }
    }

    @Test
    public void testBatchSizeAffectsAcks() throws TimeoutException, IOException {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Collections.singletonList("queue1"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");
            sender.publish("world".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.BATCH_SIZE, "1");

            runner.run(2);

            runner.assertTransferCount(ConsumeAMQP.REL_SUCCESS, 2);

            final MockFlowFile helloFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            helloFF.assertContentEquals("hello");

            final MockFlowFile worldFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(1);
            worldFF.assertContentEquals("world");

            // A single cumulative ack should be used
            assertTrue(((TestChannel) connection.createChannel()).isAck(0));
            assertTrue(((TestChannel) connection.createChannel()).isAck(1));
        }
    }

    @Test
    public void testConsumerStopped() throws TimeoutException, IOException {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Collections.singletonList("queue1"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");
            sender.publish("world".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");
            sender.publish("good-bye".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");

            LocalConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.BATCH_SIZE, "1");

            runner.run();
            proc.close();

            runner.assertTransferCount(ConsumeAMQP.REL_SUCCESS, 1);

            final MockFlowFile helloFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            helloFF.assertContentEquals("hello");


            // A single cumulative ack should be used
            assertTrue(((TestChannel) connection.createChannel()).isAck(0));

            assertFalse(((TestChannel) connection.createChannel()).isAck(1));
            assertFalse(((TestChannel) connection.createChannel()).isAck(2));

            assertFalse(connection.createChannel().isOpen());
            assertFalse(connection.isOpen());
        }
    }

    @Test
    public void validateSuccessfulConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), MessageProperties.PERSISTENT_TEXT_PLAIN, "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);

            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
        }
    }

    @Test
    public void validateHeaderWithJsonStringForHeaderFormatParameterConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Map<String, Object> headersMap = new HashMap<>();
        headersMap.put("foo1", "bar,bar");
        headersMap.put("foo2", "bar,bar");
        headersMap.put("foo3", "null");
        headersMap.put("foo4", null);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJson = objectMapper.valueToTree(headersMap);

        AMQP.BasicProperties.Builder builderBasicProperties = new AMQP.BasicProperties.Builder();
        builderBasicProperties.headers(headersMap);

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), builderBasicProperties.build(), "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.HEADER_FORMAT, OutputHeaderFormat.JSON_STRING);
            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
            String headers = successFF.getAttribute(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE);
            JsonNode jsonNode = objectMapper.readTree(headers);
            assertEquals(expectedJson, jsonNode);
        }
    }

    @Test
    public void validateHeaderWithFlowFileAttributeForHeaderFormatParameterConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Map<String, Object> expectedHeadersMap = new HashMap<>();
        expectedHeadersMap.put("foo1", "bar,bar");
        expectedHeadersMap.put("foo2", "bar,bar");
        expectedHeadersMap.put("foo3", "null");
        final Map<String, Object> headersMap = new HashMap<>(expectedHeadersMap);
        headersMap.put("foo4", null);

        final String headerPrefix = "test.header";

        AMQP.BasicProperties.Builder builderBasicProperties = new AMQP.BasicProperties.Builder();
        builderBasicProperties.headers(headersMap);

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), builderBasicProperties.build(), "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.HEADER_FORMAT, OutputHeaderFormat.ATTRIBUTES);
            runner.setProperty(ConsumeAMQP.HEADER_KEY_PREFIX, headerPrefix);
            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
            successFF.assertAttributeNotExists(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE);
            expectedHeadersMap.forEach((key, value) -> {
                successFF.assertAttributeEquals(headerPrefix + "." + key, value.toString());
            } );
        }
    }
    @Test
    public void validateHeaderWithValueSeparatorForHeaderParameterConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Map<String, Object> headersMap = new HashMap<>();
        headersMap.put("foo1", "bar,bar");
        headersMap.put("foo2", "bar,bar");
        headersMap.put("foo3", "null");
        headersMap.put("foo4", null);
        final String EXPECTED_RESULT = "{foo1=bar,bar|foo2=bar,bar|foo3=null|foo4}";

        AMQP.BasicProperties.Builder builderBasicProperties = new AMQP.BasicProperties.Builder();
        builderBasicProperties.headers(headersMap);

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), builderBasicProperties.build(), "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.HEADER_SEPARATOR, "|");
            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
            String headers = successFF.getAttribute(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE);
            assertEquals(EXPECTED_RESULT, headers);
        }
    }
    @Test
    public void validateWithNotValidHeaderSeparatorParameter() {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);
        ConsumeAMQP proc = new LocalConsumeAMQP(connection);
        TestRunner runner = initTestRunner(proc);
        runner.setProperty(ConsumeAMQP.HEADER_SEPARATOR, "|,");
        runner.assertNotValid();
    }

    @Test
    public void validateHeaderWithRemoveCurlyBracesParameterConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Map<String, Object> headersMap = new HashMap<>();
        headersMap.put("key1", "(bar,bar)");
        AMQP.BasicProperties.Builder builderBasicProperties = new AMQP.BasicProperties.Builder();
        builderBasicProperties.headers(headersMap);

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), builderBasicProperties.build(), "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.REMOVE_CURLY_BRACES, "True");
            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
            successFF.assertAttributeEquals(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE, "key1=(bar,bar)");

        }
    }

    @Test
    public void validateHeaderWithRemoveCurlyBracesAndValueSeparatorForHeaderParameterConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Map<String, Object> headersMap = new HashMap<>();
        headersMap.put("key1", "(bar,bar)");
        headersMap.put("key2", "(bar,bar)");
        final String EXPECTED_RESULT = "key1=(bar,bar)|key2=(bar,bar)";

        AMQP.BasicProperties.Builder builderBasicProperties = new AMQP.BasicProperties.Builder();
        builderBasicProperties.headers(headersMap);

        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), builderBasicProperties.build(), "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);
            runner.setProperty(ConsumeAMQP.REMOVE_CURLY_BRACES, "True");
            runner.setProperty(ConsumeAMQP.HEADER_SEPARATOR, "|");

            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
            String headers = successFF.getAttribute(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE);
            assertEquals(EXPECTED_RESULT, headers);
        }
    }

    @Test
    public void validateHeaderWithoutParameterConsumeAndTransferToSuccess() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        final Map<String, Object> headersMap = new HashMap<>();
        headersMap.put("key1", "bar");
        headersMap.put("key2", "bar2");
        headersMap.put("key3", "");
        headersMap.put("key4", null);
        final String EXPECTED_RESULT = "{key1=bar,key2=bar2,key3=,key4}";

        AMQP.BasicProperties.Builder builderBasicProperties = new AMQP.BasicProperties.Builder();
        builderBasicProperties.headers(headersMap);


        final Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), builderBasicProperties.build(), "key1", "myExchange");

            ConsumeAMQP proc = new LocalConsumeAMQP(connection);
            TestRunner runner = initTestRunner(proc);

            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeAMQP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_ROUTING_KEY_ATTRIBUTE, "key1");
            successFF.assertAttributeEquals(ConsumeAMQP.AMQP_EXCHANGE_ATTRIBUTE, "myExchange");
            String headers = successFF.getAttribute(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE);
            assertEquals(EXPECTED_RESULT, headers);
        }
    }

    private TestRunner initTestRunner(ConsumeAMQP proc) {
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ConsumeAMQP.BROKERS, "injvm:5672");
        runner.setProperty(ConsumeAMQP.QUEUE, "queue1");
        runner.setProperty(ConsumeAMQP.USER, "user");
        runner.setProperty(ConsumeAMQP.PASSWORD, "password");
        return runner;
    }

    public static class LocalConsumeAMQP extends ConsumeAMQP {
        private final Connection connection;
        private AMQPConsumer consumer;

        public LocalConsumeAMQP(Connection connection) {
            this.connection = connection;
        }

        @Override
        protected AMQPConsumer createAMQPWorker(ProcessContext context, Connection connection) {
            try {
                if (consumer != null) {
                    throw new IllegalStateException("Consumer already created");
                }

                consumer = new AMQPConsumer(connection, context.getProperty(ConsumeAMQP.QUEUE).getValue(),
                        context.getProperty(ConsumeAMQP.AUTO_ACKNOWLEDGE).asBoolean(), context.getProperty(ConsumeAMQP.PREFETCH_COUNT).asInteger(),
                        getLogger());
                return consumer;
            } catch (IOException e) {
                throw new ProcessException(e);
            }
        }

        public AMQPConsumer getAMQPWorker() {
            return consumer;
        }

        @Override
        protected Connection createConnection(ProcessContext context, ExecutorService executor) {
            return connection;
        }
    }
}
