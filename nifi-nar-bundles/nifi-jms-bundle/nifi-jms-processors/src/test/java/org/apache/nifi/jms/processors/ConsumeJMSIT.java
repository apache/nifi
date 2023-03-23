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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.jms.processors.helpers.AssertionUtils.assertCausedBy;
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
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
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
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
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
            // 1. Start a durable non shared consumer C1 with client id client1 subscribed to topic T.
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
            final MockFlowFile successFF = flowFiles.get(0);
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
            final AtomicReference<TcpTransport> tcpTransport = new AtomicReference<TcpTransport>();
            TcpTransportFactory.registerTransportFactory("validateNIFI7034", new TcpTransportFactory() {
                @Override
                protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
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
