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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties;
import org.apache.nifi.jms.processors.helpers.ConnectionFactoryInvocationHandler;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.apache.nifi.jms.processors.PublishJMS.REL_FAILURE;
import static org.apache.nifi.jms.processors.PublishJMS.REL_SUCCESS;
import static org.apache.nifi.jms.processors.helpers.AssertionUtils.assertCausedBy;
import static org.apache.nifi.jms.processors.helpers.JMSTestUtil.createJsonRecordSetReaderService;
import static org.apache.nifi.jms.processors.helpers.JMSTestUtil.createJsonRecordSetWriterService;
import static org.apache.nifi.jms.processors.ioconcept.reader.record.ProvenanceEventTemplates.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE;
import static org.apache.nifi.jms.processors.ioconcept.reader.record.ProvenanceEventTemplates.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER;
import static org.apache.nifi.jms.processors.ioconcept.reader.record.ProvenanceEventTemplates.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS;
import static org.apache.nifi.jms.processors.ioconcept.reader.StateTrackingFlowFileReader.ATTR_READ_FAILED_INDEX_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PublishJMSIT {

    TestRunner testRunner;

    @AfterEach
    public void cleanup() {
        if (testRunner != null) {
            testRunner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
            testRunner = null;
        }
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateSuccessfulPublishAndTransferToSuccess() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        final String destinationName = "validateSuccessfulPublishAndTransferToSuccess";
        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, destinationName);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        attributes.put(JmsHeaders.REPLY_TO, "cooQueue");
        attributes.put("test-attribute.type", "allowed1");
        attributes.put("test.attribute.type", "allowed2");
        attributes.put("test-attribute", "notAllowed1");
        attributes.put("jms.source.destination", "notAllowed2");
        runner.enqueue("Hey dude!".getBytes(), attributes);
        runner.run(1, false); // Run once but don't shut down because we want the Connection Factory left in tact so that we can use it.

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        BytesMessage message = (BytesMessage) jmst.receive(destinationName);

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(message);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals("cooQueue", ((Queue) message.getJMSReplyTo()).getQueueName());
        assertEquals("foo", message.getStringProperty("foo"));
        assertEquals("allowed1", message.getStringProperty("test-attribute.type"));
        assertEquals("allowed2", message.getStringProperty("test.attribute.type"));
        assertNull(message.getStringProperty("test-attribute"));
        assertNull(message.getStringProperty("jms.source.destination"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateSuccessfulPublishAndTransferToSuccessWithEL() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        final String destinationNameExpression = "${foo}Queue";
        final String destinationName = "fooQueue";
        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, destinationNameExpression);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        attributes.put(JmsHeaders.REPLY_TO, "cooQueue");
        runner.enqueue("Hey dude!".getBytes(), attributes);
        runner.run(1, false); // Run once but don't shut down because we want the Connection Factory left in tact so that we can use it.

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        BytesMessage message = (BytesMessage) jmst.receive(destinationName);

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(message);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals("cooQueue", ((Queue) message.getJMSReplyTo()).getQueueName());
        assertEquals("foo", message.getStringProperty("foo"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    @Test
    public void validateFailedPublishAndTransferToFailure() throws Exception {
        ConnectionFactory cf = mock(ConnectionFactory.class);

        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, "validateFailedPublishAndTransferToFailure");

        runner.enqueue("Hello Joe".getBytes());

        runner.run();
        Thread.sleep(200);

        assertTrue(runner.getFlowFilesForRelationship(REL_SUCCESS).isEmpty());
        assertNotNull(runner.getFlowFilesForRelationship(REL_FAILURE).get(0));
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validatePublishTextMessage() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        final String destinationName = "validatePublishTextMessage";
        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, destinationName);
        runner.setProperty(PublishJMS.MESSAGE_BODY, "text");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        attributes.put(JmsHeaders.REPLY_TO, "cooQueue");
        runner.enqueue("Hey dude!".getBytes(), attributes);
        runner.run(1, false);

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        Message message = jmst.receive(destinationName);
        assertInstanceOf(TextMessage.class, message);
        TextMessage textMessage = (TextMessage) message;

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(textMessage);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals("cooQueue", ((Queue) message.getJMSReplyTo()).getQueueName());
        assertEquals("foo", message.getStringProperty("foo"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validatePublishPropertyTypes() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        final String destinationName = "validatePublishPropertyTypes";
        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, destinationName);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        attributes.put("myboolean", "true");
        attributes.put("myboolean.type", "boolean");
        attributes.put("mybyte", "127");
        attributes.put("mybyte.type", "byte");
        attributes.put("myshort", "16384");
        attributes.put("myshort.type", "short");
        attributes.put("myinteger", "1544000");
        attributes.put("myinteger.type", "INTEGER"); // test upper case
        attributes.put("mylong", "9876543210");
        attributes.put("mylong.type", "long");
        attributes.put("myfloat", "3.14");
        attributes.put("myfloat.type", "float");
        attributes.put("mydouble", "3.14159265359");
        attributes.put("mydouble.type", "double");
        attributes.put("badtype", "3.14");
        attributes.put("badtype.type", "pi"); // pi not recognized as a type, so send as String
        attributes.put("badint", "3.14"); // value is not an integer
        attributes.put("badint.type", "integer");

        runner.enqueue("Hey dude!".getBytes(), attributes);
        runner.run(1, false); // Run once but don't shut down because we want the Connection Factory left intact so that we can use it.

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        BytesMessage message = (BytesMessage) jmst.receive(destinationName);

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(message);
        assertEquals("Hey dude!", new String(messageBytes));
        assertInstanceOf(String.class, message.getObjectProperty("foo"));
        assertEquals("foo", message.getStringProperty("foo"));
        assertInstanceOf(Boolean.class, message.getObjectProperty("myboolean"));
        assertTrue(message.getBooleanProperty("myboolean"));
        assertInstanceOf(Byte.class, message.getObjectProperty("mybyte"));
        assertEquals(127, message.getByteProperty("mybyte"));
        assertInstanceOf(Short.class, message.getObjectProperty("myshort"));
        assertEquals(16384, message.getShortProperty("myshort"));
        assertInstanceOf(Integer.class, message.getObjectProperty("myinteger"));
        assertEquals(1544000, message.getIntProperty("myinteger"));
        assertInstanceOf(Long.class, message.getObjectProperty("mylong"));
        assertEquals(9876543210L, message.getLongProperty("mylong"));
        assertInstanceOf(Float.class, message.getObjectProperty("myfloat"));
        assertEquals(3.14F, message.getFloatProperty("myfloat"), 0.001F);
        assertInstanceOf(Double.class, message.getObjectProperty("mydouble"));
        assertEquals(3.14159265359D, message.getDoubleProperty("mydouble"), 0.00000000001D);
        assertInstanceOf(String.class, message.getObjectProperty("badtype"));
        assertEquals("3.14", message.getStringProperty("badtype"));
        assertFalse(message.propertyExists("badint"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateRegexAndIllegalHeaders() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        final String destinationName = "validatePublishTextMessage";
        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, destinationName);
        runner.setProperty(PublishJMS.MESSAGE_BODY, "text");
        runner.setProperty(PublishJMS.ATTRIBUTES_AS_HEADERS_REGEX, "^((?!bar).)*$");
        runner.setProperty(PublishJMS.ALLOW_ILLEGAL_HEADER_CHARS, "true");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        attributes.put("bar", "bar");
        attributes.put("test-header-with-hyphen", "value");
        attributes.put(JmsHeaders.REPLY_TO, "cooQueue");
        runner.enqueue("Hey dude!".getBytes(), attributes);
        runner.run(1, false);

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        Message message = jmst.receive(destinationName);
        assertInstanceOf(TextMessage.class, message);
        TextMessage textMessage = (TextMessage) message;

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(textMessage);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals("cooQueue", ((Queue) message.getJMSReplyTo()).getQueueName());
        assertEquals("foo", message.getStringProperty("foo"));
        assertEquals("value", message.getStringProperty("test-header-with-hyphen"));
        assertNull(message.getStringProperty("bar"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    /**
     * <p>
     * This test validates the connection resources are closed if the publisher is marked as invalid.
     * </p>
     * <p>
     * This tests validates the proper resources handling for TCP connections using ActiveMQ (the bug was discovered against ActiveMQ 5.x). In this test, using some ActiveMQ's classes is possible to
     * verify if an opened socket is closed. See <a href="NIFI-7034">https://issues.apache.org/jira/browse/NIFI-7034</a>.
     * </p>
     * @throws Exception any error related to the broker.
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateNIFI7034() throws Exception {
        class PublishJmsForNifi7034 extends PublishJMS {
            @Override
            protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSPublisher publisher) throws ProcessException {
                super.rendezvousWithJms(context, processSession, publisher);
                publisher.setValid(false);
            }
        }
        BrokerService broker = new BrokerService();
        try {
            broker.setPersistent(false);
            broker.setBrokerName("nifi7034publisher");
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

            TestRunner runner = TestRunners.newTestRunner(new PublishJmsForNifi7034());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(cf);
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
            runner.setProperty(PublishJMS.DESTINATION, destinationName);
            runner.setProperty(PublishJMS.DESTINATION_TYPE, PublishJMS.TOPIC);

            runner.enqueue("hi".getBytes());
            runner.run();
            assertFalse(tcpTransport.get().isConnected(), "It is expected transport be closed. ");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /**
     * <p>
     * This test validates the optimal resources usage. To process one message is expected to create only one connection, one session and one message producer.
     * </p>
     * <p>
     * See <a href="NIFI-7563">https://issues.apache.org/jira/browse/NIFI-7563</a> for details.
     * </p>
     * @throws Exception any error related to the broker.
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateNIFI7563UsingOneThread() throws Exception {
        BrokerService broker = new BrokerService();
        try {
            broker.setPersistent(false);
            TransportConnector connector = broker.addConnector("tcp://127.0.0.1:0");
            int port = connector.getServer().getSocketAddress().getPort();
            broker.start();

            final ActiveMQConnectionFactory innerCf = new ActiveMQConnectionFactory("tcp://127.0.0.1:" + port);
            ConnectionFactoryInvocationHandler connectionFactoryProxy = new ConnectionFactoryInvocationHandler(innerCf);

            // Create a connection Factory proxy to catch metrics and usage.
            ConnectionFactory cf = (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(), new Class[] {ConnectionFactory.class}, connectionFactoryProxy);

            TestRunner runner = TestRunners.newTestRunner(new PublishJMS());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(cf);
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");

            String destinationName = "myDestinationName";
            // The destination option according current implementation should contain topic or queue to infer the destination type
            // from the name. Check https://issues.apache.org/jira/browse/NIFI-7561. Once that is fixed, the name can be
            // randomly created.
            String topicNameInHeader = "topic-foo";
            runner.setProperty(PublishJMS.DESTINATION, destinationName);
            runner.setProperty(PublishJMS.DESTINATION_TYPE, PublishJMS.QUEUE);

            int threads = 1;
            Map<String, String> flowFileAttributes = new HashMap<>();
            // This method will be removed once https://issues.apache.org/jira/browse/NIFI-7564 is fixed.
            flowFileAttributes.put(JmsHeaders.DESTINATION, topicNameInHeader);
            flowFileAttributes.put(JmsHeaders.REPLY_TO, topicNameInHeader);
            runner.setThreadCount(threads);
            runner.enqueue("hi".getBytes(), flowFileAttributes);
            runner.enqueue("hi".getBytes(), flowFileAttributes);
            runner.run(2);
            assertEquals(threads, connectionFactoryProxy.openedConnections(), "It is expected at least " + threads + " Connection to be opened.");
            assertEquals(threads, connectionFactoryProxy.openedSessions(), "It is expected " + threads + " Session to be opened and there are " + connectionFactoryProxy.openedSessions());
            assertEquals(threads, connectionFactoryProxy.openedProducers(), "It is expected " + threads + " MessageProducer to be opened and there are " + connectionFactoryProxy.openedProducers());
            assertTrue(connectionFactoryProxy.isAllResourcesClosed(), "Some resources were not closed.");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /**
     * <p>
     * This test validates the optimal resources usage. To process one message is expected to create only one connection, one session and one message producer.
     * </p>
     * <p>
     * See <a href="NIFI-7563">https://issues.apache.org/jira/browse/NIFI-7563</a> for details.
     * </p>
     * @throws Exception any error related to the broker.
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void validateNIFI7563UsingMultipleThreads() throws Exception {
        BrokerService broker = new BrokerService();
        try {
            broker.setPersistent(false);
            TransportConnector connector = broker.addConnector("tcp://127.0.0.1:0");
            int port = connector.getServer().getSocketAddress().getPort();
            broker.start();

            final ActiveMQConnectionFactory innerCf = new ActiveMQConnectionFactory("tcp://127.0.0.1:" + port);
            ConnectionFactoryInvocationHandler connectionFactoryProxy = new ConnectionFactoryInvocationHandler(innerCf);

            // Create a connection Factory proxy to catch metrics and usage.
            ConnectionFactory cf = (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(), new Class[] {ConnectionFactory.class}, connectionFactoryProxy);

            TestRunner runner = TestRunners.newTestRunner(new PublishJMS());
            JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
            when(cs.getIdentifier()).thenReturn("cfProvider");
            when(cs.getConnectionFactory()).thenReturn(cf);
            runner.addControllerService("cfProvider", cs);
            runner.enableControllerService(cs);

            runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");

            String destinationName = "myDestinationName";
            // The destination option according current implementation should contain topic or queue to infer the destination type
            // from the name. Check https://issues.apache.org/jira/browse/NIFI-7561. Once that is fixed, the name can be
            // randomly created.
            String topicNameInHeader = "topic-foo";
            runner.setProperty(PublishJMS.DESTINATION, destinationName);
            runner.setProperty(PublishJMS.DESTINATION_TYPE, PublishJMS.QUEUE);

            int messagesToGenerate = 1000;
            int threads = 10;
            runner.setThreadCount(threads);
            Map<String, String> flowFileAttributes = new HashMap<>();
            // This method will be removed once https://issues.apache.org/jira/browse/NIFI-7564 is fixed.
            flowFileAttributes.put(JmsHeaders.DESTINATION, topicNameInHeader);
            flowFileAttributes.put(JmsHeaders.REPLY_TO, topicNameInHeader);
            byte[] messageContent = "hi".getBytes();
            for (int i = 0; i < messagesToGenerate; i++) {
                runner.enqueue(messageContent, flowFileAttributes);
            }
            runner.run(messagesToGenerate);
            assertTrue(connectionFactoryProxy.openedConnections() <= threads, "It is expected at least " + threads + " Connection to be opened.");
            assertTrue(connectionFactoryProxy.openedSessions() <= threads, "It is expected " + threads + " Session to be opened and there are " + connectionFactoryProxy.openedSessions());
            assertTrue(connectionFactoryProxy.openedProducers() <= threads, "It is expected " + threads + " MessageProducer to be opened and there are " + connectionFactoryProxy.openedProducers());
            assertTrue(connectionFactoryProxy.isAllResourcesClosed(), "Some resources were not closed.");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    @Test
    public void whenExceptionIsRaisedDuringConnectionFactoryInitializationTheProcessorShouldBeYielded() {
        final String nonExistentClassName = "DummyInitialContextFactoryClass";

        TestRunner runner = TestRunners.newTestRunner(PublishJMS.class);

        // using JNDI JMS Connection Factory configured locally on the processor
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, nonExistentClassName);
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, "DummyProviderUrl");
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, "DummyConnectionFactoryName");

        runner.setProperty(AbstractJMSProcessor.DESTINATION, "myTopic");
        runner.setProperty(AbstractJMSProcessor.DESTINATION_TYPE, AbstractJMSProcessor.TOPIC);

        runner.enqueue("message");

        assertCausedBy(ClassNotFoundException.class, nonExistentClassName, runner::run);

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled(), "In case of an exception, the processor should be yielded.");
    }

    @Test
    public void testPublishRecords() throws InitializationException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        String destination = "testPublishRecords";
        testRunner = initializeTestRunner(cf, destination);
        testRunner.setProperty(PublishJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.assertValid();

        final ArrayNode testInput = createTestJsonInput();

        testRunner.enqueue(testInput.toString().getBytes());

        testRunner.run(1, false); // Run once but don't shut down because we want the Connection Factory left intact so that we can use it.

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS, 3));

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(cf, destination, testInput.get(0).toString());
        verifyPublishedMessage(cf, destination, testInput.get(1).toString());
        verifyPublishedMessage(cf, destination, testInput.get(2).toString());

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile successfulFlowFile = flowFiles.get(0);
        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_READ_FAILED_INDEX_SUFFIX;
        assertFalse(successfulFlowFile.getAttributes().containsKey(publishFailedIndexAttributeName), "Failed attribute should not be present on the FlowFile");
    }

    @Test
    public void testPublishRecordsFailed() throws InitializationException {
        PublishJMS processor = new PublishJMS() {
            @Override
            protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSPublisher publisher) throws ProcessException {
                JMSPublisher spiedPublisher = Mockito.spy(publisher);
                Mockito.doCallRealMethod()
                        .doThrow(new RuntimeException("Second publish failed."))
                        .when(spiedPublisher).publish(any(), any(byte[].class), any());
                super.rendezvousWithJms(context, processSession, spiedPublisher);
            }
        };

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        String destination = "testPublishRecords";
        testRunner = initializeTestRunner(processor, cf, destination);
        testRunner.setProperty(PublishJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.assertValid();

        final ArrayNode testInput = createTestJsonInput();

        testRunner.enqueue(testInput.toString().getBytes());

        testRunner.run(1, false); // Run once but don't shut down because we want the Connection Factory left intact so that we can use it.

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, 1));

        verifyPublishedMessage(cf, destination, testInput.get(0).toString());

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals(1, flowFiles.size());

        final MockFlowFile failedFlowFile = flowFiles.get(0);
        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_READ_FAILED_INDEX_SUFFIX;
        assertEquals("1", failedFlowFile.getAttribute(publishFailedIndexAttributeName), "Only one record is expected to be published successfully.");
    }

    @Test
    public void testContinuePublishRecordsAndFailAgainWhenPreviousPublishFailed() throws InitializationException {
        PublishJMS processor = new PublishJMS() {
            @Override
            protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSPublisher publisher) throws ProcessException {
                JMSPublisher spiedPublisher = Mockito.spy(publisher);
                Mockito.doCallRealMethod()
                        .doThrow(new RuntimeException("Second publish failed."))
                        .when(spiedPublisher).publish(any(), any(byte[].class), any());
                super.rendezvousWithJms(context, processSession, spiedPublisher);
            }
        };

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        String destination = "testPublishRecords";
        testRunner = initializeTestRunner(processor, cf, destination);
        testRunner.setProperty(PublishJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.assertValid();

        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_READ_FAILED_INDEX_SUFFIX;
        final ArrayNode testInput = createTestJsonInput();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(publishFailedIndexAttributeName, "1");
        testRunner.enqueue(testInput.toString().getBytes(), attributes);

        testRunner.run(1, false); // Run once but don't shut down because we want the Connection Factory left intact so that we can use it.

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, 2));

        verifyPublishedMessage(cf, destination, testInput.get(1).toString());

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals(1, flowFiles.size());

        final MockFlowFile failedFlowFile = flowFiles.get(0);
        assertEquals("2", failedFlowFile.getAttribute(publishFailedIndexAttributeName), "Only one record is expected to be published successfully.");
    }

    @Test
    public void testContinuePublishRecordsSuccessfullyWhenPreviousPublishFailed() throws InitializationException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        String destination = "testPublishRecords";
        testRunner = initializeTestRunner(cf, destination);
        testRunner.setProperty(PublishJMS.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishJMS.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.assertValid();

        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_READ_FAILED_INDEX_SUFFIX;
        final ArrayNode testInput = createTestJsonInput();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(publishFailedIndexAttributeName, "1");
        testRunner.enqueue(testInput.toString().getBytes(), attributes);

        testRunner.run(1, false); // Run once but don't shut down because we want the Connection Factory left intact so that we can use it.

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER, 3));

        verifyPublishedMessage(cf, destination, testInput.get(1).toString());
        verifyPublishedMessage(cf, destination, testInput.get(2).toString());

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile successfulFlowFile = flowFiles.get(0);
        assertNull(successfulFlowFile.getAttribute(publishFailedIndexAttributeName),
                publishFailedIndexAttributeName + " is expected to be removed after all remaining records have been published successfully.");
    }

    private TestRunner initializeTestRunner(ConnectionFactory connectionFactory, String destinationName) throws InitializationException {
        PublishJMS processor = new PublishJMS();
        return initializeTestRunner(processor, connectionFactory, destinationName);
    }

    private TestRunner initializeTestRunner(PublishJMS processor, ConnectionFactory connectionFactory, String destinationName) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(processor);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(connectionFactory);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, destinationName);

        return runner;
    }

    private void verifyPublishedMessage(ConnectionFactory connectionFactory, String destinationName, String content) {
        JmsTemplate jmst = new JmsTemplate(connectionFactory);
        BytesMessage message = (BytesMessage) jmst.receive(destinationName);

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(message);
        assertEquals(content, new String(messageBytes));
    }

    private ProvenanceEventRecord assertProvenanceEvent() {
        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertNotNull(provenanceEvents);
        assertEquals(1, provenanceEvents.size());

        final ProvenanceEventRecord event = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, event.getEventType());

        return event;
    }

    private void assertProvenanceEvent(String expectedDetails) {
        final ProvenanceEventRecord event = assertProvenanceEvent();
        assertEquals(expectedDetails, event.getDetails());
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
}
