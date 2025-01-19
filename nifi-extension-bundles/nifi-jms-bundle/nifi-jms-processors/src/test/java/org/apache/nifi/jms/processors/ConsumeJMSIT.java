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
package org.apache.nifi.jms.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProperties;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.jms.processors.ioconcept.writer.record.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.support.JmsHeaders;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.apache.nifi.jms.processors.helpers.AssertionUtils.assertCausedBy;
import static org.apache.nifi.jms.processors.helpers.JMSTestUtil.createJsonRecordSetReaderService;
import static org.apache.nifi.jms.processors.helpers.JMSTestUtil.createJsonRecordSetWriterService;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConsumeJMSIT {

    private static final String JMS_DESTINATION_ATTRIBUTE_NAME = "jms_destination";

    @Test
    public void validateSuccessfulConsumeAndTransferToSuccess() throws Exception {
        final String destinationName = "cooQueue";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            JMSPublisher sender = new JMSPublisher((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            final Map<String, String> senderAttributes = new HashMap<>();
            senderAttributes.put("filename", "message.txt");
            senderAttributes.put("attribute_from_sender", "some value");
            sender.publish(destinationName, "Hey dude!".getBytes(), senderAttributes);
            TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(ConsumeJMS.DESTINATION, destinationName);
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);

            runner.run(1, false);
            //
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).getFirst();
            assertNotNull(successFF);
            successFF.assertAttributeExists(JmsHeaders.DESTINATION);
            successFF.assertAttributeEquals(JmsHeaders.DESTINATION, destinationName);
            successFF.assertAttributeExists("filename");
            successFF.assertAttributeEquals("filename", "message.txt");
            successFF.assertAttributeExists("attribute_from_sender");
            successFF.assertAttributeEquals("attribute_from_sender", "some value");
            successFF.assertAttributeExists("jms.messagetype");
            successFF.assertAttributeEquals("jms.messagetype", "BytesMessage");
            successFF.assertContentEquals("Hey dude!".getBytes());
            String sourceDestination = successFF.getAttribute(ConsumeJMS.JMS_SOURCE_DESTINATION_NAME);
            assertNotNull(sourceDestination);
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testValidateErrorQueueWhenDestinationIsTopicAndErrorQueueIsSet() throws Exception {
        testValidateErrorQueue(ConsumeJMS.TOPIC, "errorQueue", false);
    }

    @Test
    public void testValidateErrorQueueWhenDestinationIsTopicAndErrorQueueIsNotSet() throws Exception {
        testValidateErrorQueue(ConsumeJMS.TOPIC, null, true);
    }

    @Test
    public void testValidateErrorQueueWhenDestinationIsQueueAndErrorQueueIsSet() throws Exception {
        testValidateErrorQueue(ConsumeJMS.QUEUE, "errorQueue", true);
    }

    @Test
    public void testValidateErrorQueueWhenDestinationIsQueueAndErrorQueueIsNotSet() throws Exception {
        testValidateErrorQueue(ConsumeJMS.QUEUE, null, true);
    }

    private void testValidateErrorQueue(String destinationType, String errorQueue, boolean expectedValid) throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        try {
            TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());

            JMSConnectionFactoryProviderDefinition cfService = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cfService.getIdentifier()).thenReturn("cfService");
            when(cfService.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());

            runner.addControllerService("cfService", cfService);
            runner.enableControllerService(cfService);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfService");
            runner.setProperty(ConsumeJMS.DESTINATION, "destination");
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, destinationType);
            if (errorQueue != null) {
                runner.setProperty(ConsumeJMS.ERROR_QUEUE, errorQueue);
            }

            if (expectedValid) {
                runner.assertValid();
            } else {
                runner.assertNotValid();
            }
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testTextMessageTypeAttribute() throws Exception {
        testMessageTypeAttribute("testTextMessage", Session::createTextMessage, TextMessage.class.getSimpleName());
    }

    @Test
    public void testByteMessageTypeAttribute() throws Exception {
        testMessageTypeAttribute("testByteMessage", Session::createBytesMessage, BytesMessage.class.getSimpleName());
    }

    @Test
    public void testObjectMessageTypeAttribute() throws Exception {
        String destinationName = "testObjectMessage";

        testMessageTypeAttribute(destinationName, Session::createObjectMessage, ObjectMessage.class.getSimpleName());
    }

    @Test
    public void testStreamMessageTypeAttribute() throws Exception {
        testMessageTypeAttribute("testStreamMessage", Session::createStreamMessage, StreamMessage.class.getSimpleName());
    }

    @Test
    public void testMapMessageTypeAttribute() throws Exception {
        testMessageTypeAttribute("testMapMessage", Session::createMapMessage, MapMessage.class.getSimpleName());
    }

    @Test
    public void testUnsupportedMessage() throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

            jmsTemplate.send("testMapMessage", __ -> createUnsupportedMessage("unsupportedMessagePropertyKey", "unsupportedMessagePropertyValue"));

            TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(ConsumeJMS.DESTINATION, "testMapMessage");
            runner.setProperty(ConsumeJMS.ERROR_QUEUE, "errorQueue");
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);
            runner.run(1, false);

            JmsTemplate jmst = new JmsTemplate(cf);
            Message message = jmst.receive("errorQueue");

            assertNotNull(message);
            assertEquals(message.getStringProperty("unsupportedMessagePropertyKey"), "unsupportedMessagePropertyValue");
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    private void testMessageTypeAttribute(String destinationName, final MessageCreator messageCreator, String expectedJmsMessageTypeAttribute) throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            jmsTemplate.send(destinationName, messageCreator);

            TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(ConsumeJMS.DESTINATION, destinationName);
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);
            runner.run(1, false);
            //
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).getFirst();
            assertNotNull(successFF);

            successFF.assertAttributeExists(ConsumeJMS.JMS_MESSAGETYPE);
            successFF.assertAttributeEquals(ConsumeJMS.JMS_MESSAGETYPE, expectedJmsMessageTypeAttribute);
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    public ActiveMQMessage createUnsupportedMessage(String propertyKey, String propertyValue) throws JMSException {
        ActiveMQMessage message = new ActiveMQMessage();

        message.setStringProperty(propertyKey, propertyValue);

        return message;
    }

    /**
     * Validates <a href="https://issues.apache.org/jira/browse/NIFI-6915">NIFI-6915</a>.
     * <p>
     * The test consists on:
     * <ul>
     * <li>Start a durable non shared consumer <tt>C1</tt> with client id <tt>client1</tt> subscribed to topic <tt>T</tt>.</li>
     * <li>Stop <tt>C1</tt>.</li>
     * <li>Publish a message <tt>M1</tt> to topic <tt>T</tt>.</li>
     * <li>Start <tt>C1</tt>.</li>
     * </ul>
     * It is expected <tt>C1</tt> receives message <tt>M1</tt>.
     * </p>
     * @throws Exception
     *             unexpected
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateNifi6915() throws Exception {
        BrokerService broker = new BrokerService();
        try {
            broker.setPersistent(false);
            broker.setBrokerName("broker1");
            broker.start();
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://broker1");
            final String destinationName = "validateNifi6915";

            TestRunner c1Consumer = createNonSharedDurableConsumer(cf, destinationName);
            // 1. Start a durable non-shared consumer C1 with client id client1 subscribed to topic T.
            boolean stopConsumer = true;
            c1Consumer.run(1, stopConsumer);
            List<MockFlowFile> flowFiles = c1Consumer.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertTrue(flowFiles.isEmpty(), "Expected no messages");
            // 2. Publish a message M1 to topic T.
            publishAMessage(cf, destinationName, "Hi buddy!!");
            // 3. Start C1.
            c1Consumer.run(1, true);
            flowFiles = c1Consumer.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertEquals(1, flowFiles.size());

            // It is expected C1 receives message M1.
            final MockFlowFile successFF = flowFiles.getFirst();
            assertNotNull(successFF);
            successFF.assertAttributeExists(JmsHeaders.DESTINATION);
            successFF.assertAttributeEquals(JmsHeaders.DESTINATION, destinationName);
            successFF.assertContentEquals("Hi buddy!!".getBytes());
            assertEquals(destinationName, successFF.getAttribute(ConsumeJMS.JMS_SOURCE_DESTINATION_NAME));
        } catch (Exception e) {
            throw e;
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateNifi6915OnlyOneThreadAllowed() {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        final String destinationName = "validateNifi6915";

        TestRunner runner = createNonSharedDurableConsumer(cf, destinationName);
        runner.setThreadCount(2);
        final TestRunner temp = runner;

        assertCausedBy(ProcessException.class, "Durable non shared subscriptions cannot work on multiple threads.", () -> temp.run(1, true));

        runner = createNonSharedDurableConsumer(cf, destinationName);
        // using one thread, it should not fail.
        runner.setThreadCount(1);
        runner.run(1, true);
    }

    /**
     * <p>
     * This test validates the connection resources are closed if the consumer is marked as invalid.
     * </p>
     * <p>
     * This tests validates the proper resources handling for TCP connections using ActiveMQ (the bug was discovered against ActiveMQ 5.x). In this test, using some ActiveMQ's classes is possible to
     * verify if an opened socket is closed. See <a href="https://issues.apache.org/jira/browse/NIFI-7034">NIFI-7034</a>.
     * </p>
     * @throws Exception
     *             any error related to the broker.
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateNIFI7034() throws Exception {
        class ConsumeJMSForNifi7034 extends ConsumeJMS {
            @Override
            protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSConsumer consumer) throws ProcessException {
                super.rendezvousWithJms(context, processSession, consumer);
                consumer.setValid(false);
            }
        }
        BrokerService broker = new BrokerService();
        try {
            broker.setPersistent(false);
            broker.setBrokerName("nifi7034consumer");
            TransportConnector connector = broker.addConnector("tcp://127.0.0.1:0");
            int port = connector.getServer().getSocketAddress().getPort();
            broker.start();

            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("validateNIFI7034://127.0.0.1:" + port);
            final String destinationName = "nifi7034";
            final AtomicReference<TcpTransport> tcpTransport = new AtomicReference<>();
            TcpTransportFactory.registerTransportFactory("validateNIFI7034", new TcpTransportFactory() {
                @Override
                protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws IOException {
                    TcpTransport transport = super.createTcpTransport(wf, socketFactory, location, localLocation);
                    tcpTransport.set(transport);
                    return transport;
                }
            });

            TestRunner runner = TestRunners.newTestRunner(new ConsumeJMSForNifi7034());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(cf);
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(ConsumeJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(ConsumeJMS.DESTINATION, destinationName);
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.TOPIC);

            runner.run();
            // since the worker is marked to invalid, we don't need to expect an exception here, because the worker recreation is handled automatically

            assertFalse(tcpTransport.get().isConnected(), "It is expected transport be closed. ");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void whenExceptionIsRaisedTheProcessorShouldBeYielded() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://invalidhost:9999?soTimeout=3");

        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);
        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(ConsumeJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(ConsumeJMS.DESTINATION, "foo");
        runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.TOPIC);

        assertCausedBy(UnknownHostException.class, runner::run);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled(), "In case of an exception, the processor should be yielded.");
    }

    @Test
    public void whenExceptionIsRaisedDuringConnectionFactoryInitializationTheProcessorShouldBeYielded() throws Exception {
        final String nonExistentClassName = "DummyJMSConnectionFactoryClass";

        TestRunner runner = TestRunners.newTestRunner(ConsumeJMS.class);

        // using (non-JNDI) JMS Connection Factory via controller service
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, nonExistentClassName);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, "DummyBrokerUri");
        runner.enableControllerService(cfProvider);

        runner.setProperty(ConsumeJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(ConsumeJMS.DESTINATION, "myTopic");
        runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.TOPIC);

        assertCausedBy(ClassNotFoundException.class, nonExistentClassName, runner::run);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled(), "In case of an exception, the processor should be yielded.");
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void whenExceptionIsRaisedInAcceptTheProcessorShouldYieldAndRollback() throws Exception {
        final String destination = "testQueue";
        final RuntimeException expectedException = new RuntimeException();

        final ConsumeJMS processor = new ConsumeJMS() {
            @Override
            protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSConsumer consumer) throws ProcessException {
                ProcessSession spiedSession = spy(processSession);
                doThrow(expectedException).when(spiedSession).write(any(FlowFile.class), any(OutputStreamCallback.class));
                super.rendezvousWithJms(context, spiedSession, consumer);
            }
        };

        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            jmsTemplate.send(destination, session -> session.createTextMessage("msg"));

            TestRunner runner = TestRunners.newTestRunner(processor);
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(ConsumeJMS.DESTINATION, destination);
            runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);

            assertCausedBy(expectedException, () -> runner.run(1, false));

            assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled(), "In case of an exception, the processor should be yielded.");
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testConsumeRecords() throws InitializationException {
        String destination = "testConsumeRecords";
        ArrayNode expectedRecordSet = createTestJsonInput();

        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedRecordSet.get(0).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedRecordSet.get(1).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedRecordSet.get(2).toString()));

            TestRunner testRunner = initializeTestRunner(jmsTemplate.getConnectionFactory(), destination);
            testRunner.setProperty(ConsumeJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
            testRunner.setProperty(ConsumeJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
            testRunner.setProperty(AbstractJMSProcessor.MAX_BATCH_SIZE, "10");

            testRunner.run(1, false);

            List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertEquals(1, successFlowFiles.size());
            assertEquals(expectedRecordSet.toString(), new String(successFlowFiles.getFirst().toByteArray()));

            List<MockFlowFile> parseFailedFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_PARSE_FAILURE);
            assertEquals(0, parseFailedFlowFiles.size());
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testConsumeMalformedRecords() throws InitializationException {
        String destination = "testConsumeRecords";
        ArrayNode expectedRecordSet = createTestJsonInput();
        String expectedParseFailedContent1 = "this is not a json";
        String expectedParseFailedContent2 = "this is still not a json";

        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedRecordSet.get(0).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedParseFailedContent1));
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedRecordSet.get(1).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedParseFailedContent2));
            jmsTemplate.send(destination, session -> session.createTextMessage(expectedRecordSet.get(2).toString()));

            TestRunner testRunner = initializeTestRunner(jmsTemplate.getConnectionFactory(), destination);
            testRunner.setProperty(ConsumeJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
            testRunner.setProperty(ConsumeJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
            testRunner.setProperty(AbstractJMSProcessor.MAX_BATCH_SIZE, "10");
            testRunner.setRelationshipAvailable(ConsumeJMS.REL_PARSE_FAILURE);

            testRunner.run(1, false);

            // checking whether the processor was able to construct a valid recordSet from the properly formatted messages
            List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertEquals(1, successFlowFiles.size());
            assertEquals(expectedRecordSet.toString(), new String(successFlowFiles.getFirst().toByteArray()));

            // and checking whether it creates separate FlowFiles for the malformed messages
            List<MockFlowFile> parseFailedFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_PARSE_FAILURE);
            assertEquals(2, parseFailedFlowFiles.size());
            assertEquals(expectedParseFailedContent1, new String(parseFailedFlowFiles.get(0).toByteArray()));
            assertEquals(expectedParseFailedContent2, new String(parseFailedFlowFiles.get(1).toByteArray()));
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testConsumeRecordsWithAppenderOutputStrategy() throws InitializationException, JsonProcessingException {
        String destination = "testConsumeRecordsWithAppenderOutputStrategy";
        ArrayNode inputRecordSet = createTestJsonInput();

        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            jmsTemplate.send(destination, session -> session.createTextMessage(inputRecordSet.get(0).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(inputRecordSet.get(1).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(inputRecordSet.get(2).toString()));

            TestRunner testRunner = initializeTestRunner(jmsTemplate.getConnectionFactory(), destination);
            testRunner.setProperty(ConsumeJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
            testRunner.setProperty(ConsumeJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
            testRunner.setProperty(AbstractJMSProcessor.MAX_BATCH_SIZE, "10");
            testRunner.setProperty(ConsumeJMS.OUTPUT_STRATEGY, OutputStrategy.USE_APPENDER.getValue());

            testRunner.run(1, false);

            List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertEquals(1, successFlowFiles.size());
            JsonNode flowFileContentAsJson = deserializeToJsonNode(new String(successFlowFiles.getFirst().toByteArray()));
            // checking that the output contains at least a part of the original input
            assertEquals(inputRecordSet.get(0).get("firstAttribute").asText(), flowFileContentAsJson.get(0).get("firstAttribute").asText());
            assertEquals(inputRecordSet.get(1).get("firstAttribute").asText(), flowFileContentAsJson.get(1).get("firstAttribute").asText());
            assertEquals(inputRecordSet.get(2).get("firstAttribute").asText(), flowFileContentAsJson.get(2).get("firstAttribute").asText());
            // checking jms_destination attribute exists with the given value
            // this attribute has been chosen because it is deterministic; others vary based on host, time, etc.
            // not nice, but stubbing all attributes would be uglier with the current code structure
            assertEquals(destination, flowFileContentAsJson.get(0).get("_" + JMS_DESTINATION_ATTRIBUTE_NAME).asText());
            assertEquals(destination, flowFileContentAsJson.get(1).get("_" + JMS_DESTINATION_ATTRIBUTE_NAME).asText());
            assertEquals(destination, flowFileContentAsJson.get(2).get("_" + JMS_DESTINATION_ATTRIBUTE_NAME).asText());

            List<MockFlowFile> parseFailedFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_PARSE_FAILURE);
            assertEquals(0, parseFailedFlowFiles.size());
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testConsumeRecordsWithWrapperOutputStrategy() throws InitializationException, JsonProcessingException {
        String destination = "testConsumeRecordsWithWrapperOutputStrategy";
        String valueKey = "value";
        String attributeKey = "_";
        ArrayNode inputRecordSet = createTestJsonInput();

        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            jmsTemplate.send(destination, session -> session.createTextMessage(inputRecordSet.get(0).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(inputRecordSet.get(1).toString()));
            jmsTemplate.send(destination, session -> session.createTextMessage(inputRecordSet.get(2).toString()));

            TestRunner testRunner = initializeTestRunner(jmsTemplate.getConnectionFactory(), destination);
            testRunner.setProperty(ConsumeJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
            testRunner.setProperty(ConsumeJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
            testRunner.setProperty(AbstractJMSProcessor.MAX_BATCH_SIZE, "10");
            testRunner.setProperty(ConsumeJMS.OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER.getValue());

            testRunner.run(1, false);

            List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS);
            assertEquals(1, successFlowFiles.size());
            JsonNode flowFileContentAsJson = deserializeToJsonNode(new String(successFlowFiles.getFirst().toByteArray()));
            // checking that the original json is equal to the leaf
            assertEquals(inputRecordSet.get(0), flowFileContentAsJson.get(0).get(valueKey));
            assertEquals(inputRecordSet.get(1), flowFileContentAsJson.get(1).get(valueKey));
            assertEquals(inputRecordSet.get(2), flowFileContentAsJson.get(2).get(valueKey));
            // checking that the attribute leaf contains at least the jms_destination attribute
            // this attribute has been chosen because it is deterministic; others vary based on host, time, etc.
            // not nice, but stubbing all attributes would be uglier with the current code structure
            assertEquals(destination, flowFileContentAsJson.get(0).get(attributeKey).get(JMS_DESTINATION_ATTRIBUTE_NAME).asText());
            assertEquals(destination, flowFileContentAsJson.get(1).get(attributeKey).get(JMS_DESTINATION_ATTRIBUTE_NAME).asText());
            assertEquals(destination, flowFileContentAsJson.get(2).get(attributeKey).get(JMS_DESTINATION_ATTRIBUTE_NAME).asText());

            List<MockFlowFile> parseFailedFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeJMS.REL_PARSE_FAILURE);
            assertEquals(0, parseFailedFlowFiles.size());
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    private static ArrayNode createTestJsonInput() {
        final ObjectMapper mapper = new ObjectMapper();

        return mapper.createArrayNode().addAll(asList(
                mapper.createObjectNode()
                        .put("recordId", 1)
                        .put("firstAttribute", "foo")
                        .put("secondAttribute", false),
                mapper.createObjectNode()
                        .put("recordId", 2)
                        .put("firstAttribute", "bar")
                        .put("secondAttribute", true),
                mapper.createObjectNode()
                        .put("recordId", 3)
                        .put("firstAttribute", "foobar")
                        .put("secondAttribute", false)
        ));
    }

    private JsonNode deserializeToJsonNode(String rawJson) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(rawJson);
    }

    private TestRunner initializeTestRunner(ConnectionFactory connectionFactory, String destinationName) throws InitializationException {
        return initializeTestRunner(new ConsumeJMS(), connectionFactory, destinationName);
    }

    private TestRunner initializeTestRunner(ConsumeJMS processor, ConnectionFactory connectionFactory, String destinationName) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(processor);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(connectionFactory);
        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(ConsumeJMS.DESTINATION, destinationName);
        runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);

        return runner;
    }

    private static void publishAMessage(ActiveMQConnectionFactory cf, final String destinationName, String messageContent) throws JMSException {
        // Publish a message.
        try (Connection conn = cf.createConnection();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createTopic(destinationName))) {
            producer.send(session.createTextMessage(messageContent));
        }
    }

    private static TestRunner createNonSharedDurableConsumer(ActiveMQConnectionFactory cf, final String destinationName) {
        ConsumeJMS c1 = new ConsumeJMS();
        TestRunner c1Consumer = TestRunners.newTestRunner(c1);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        try {
            c1Consumer.addControllerService("cfProvider", cs);
        } catch (InitializationException e) {
            throw new IllegalStateException(e);
        }
        c1Consumer.enableControllerService(cs);

        c1Consumer.setProperty(ConsumeJMS.CF_SERVICE, "cfProvider");
        c1Consumer.setProperty(ConsumeJMS.DESTINATION, destinationName);
        c1Consumer.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.TOPIC);
        c1Consumer.setProperty(ConsumeJMS.DURABLE_SUBSCRIBER, "true");
        c1Consumer.setProperty(ConsumeJMS.SUBSCRIPTION_NAME, "SubscriptionName");
        c1Consumer.setProperty(ConsumeJMS.SHARED_SUBSCRIBER, "false");
        c1Consumer.setProperty(ConsumeJMS.CLIENT_ID, "client1");
        return c1Consumer;
    }

}
