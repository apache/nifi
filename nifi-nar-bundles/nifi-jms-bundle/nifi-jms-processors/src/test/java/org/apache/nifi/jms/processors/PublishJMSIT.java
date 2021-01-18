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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.net.SocketFactory;

public class PublishJMSIT {

    @Test(timeout = 10000)
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

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
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

    @Test(timeout = 10000)
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

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
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

        assertTrue(runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).isEmpty());
        assertNotNull(runner.getFlowFilesForRelationship(PublishJMS.REL_FAILURE).get(0));
    }

    @Test(timeout = 10000)
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

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        Message message = jmst.receive(destinationName);
        assertTrue(message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(textMessage);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals("cooQueue", ((Queue) message.getJMSReplyTo()).getQueueName());
        assertEquals("foo", message.getStringProperty("foo"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    @Test(timeout = 10000)
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

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        BytesMessage message = (BytesMessage) jmst.receive(destinationName);

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(message);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals(true, message.getObjectProperty("foo") instanceof String);
        assertEquals("foo", message.getStringProperty("foo"));
        assertEquals(true, message.getObjectProperty("myboolean") instanceof Boolean);
        assertEquals(true, message.getBooleanProperty("myboolean"));
        assertEquals(true, message.getObjectProperty("mybyte") instanceof Byte);
        assertEquals(127, message.getByteProperty("mybyte"));
        assertEquals(true, message.getObjectProperty("myshort") instanceof Short);
        assertEquals(16384, message.getShortProperty("myshort"));
        assertEquals(true, message.getObjectProperty("myinteger") instanceof Integer);
        assertEquals(1544000, message.getIntProperty("myinteger"));
        assertEquals(true, message.getObjectProperty("mylong") instanceof Long);
        assertEquals(9876543210L, message.getLongProperty("mylong"));
        assertEquals(true, message.getObjectProperty("myfloat") instanceof Float);
        assertEquals(3.14F, message.getFloatProperty("myfloat"), 0.001F);
        assertEquals(true, message.getObjectProperty("mydouble") instanceof Double);
        assertEquals(3.14159265359D, message.getDoubleProperty("mydouble"), 0.00000000001D);
        assertEquals(true, message.getObjectProperty("badtype") instanceof String);
        assertEquals("3.14", message.getStringProperty("badtype"));
        assertFalse(message.propertyExists("badint"));

        runner.run(1, true, false); // Run once just so that we can trigger the shutdown of the Connection Factory
    }

    @Test(timeout = 10000)
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

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        Message message = jmst.receive(destinationName);
        assertTrue(message instanceof TextMessage);
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
    @Test(timeout = 10000)
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
            final AtomicReference<TcpTransport> tcpTransport = new AtomicReference<TcpTransport>();
            TcpTransportFactory.registerTransportFactory("validateNIFI7034", new TcpTransportFactory() {
                @Override
                protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
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
            assertFalse("It is expected transport be closed. ", tcpTransport.get().isConnected());
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
    @Test(timeout = 10000)
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
            ConnectionFactory cf = (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(), new Class[] { ConnectionFactory.class }, connectionFactoryProxy);

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
            assertTrue("It is expected at least " + threads + " Connection to be opened.", threads == connectionFactoryProxy.openedConnections());
            assertTrue("It is expected " + threads + " Session to be opened and there are " + connectionFactoryProxy.openedSessions(), threads == connectionFactoryProxy.openedSessions());
            assertTrue("It is expected " + threads + " MessageProducer to be opened and there are " + connectionFactoryProxy.openedProducers(), threads == connectionFactoryProxy.openedProducers());
            assertTrue("Some resources were not closed.", connectionFactoryProxy.isAllResourcesClosed());
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
    @Test(timeout = 10000)
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
            ConnectionFactory cf = (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(), new Class[] { ConnectionFactory.class }, connectionFactoryProxy);

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
            assertTrue("It is expected at least " + threads + " Connection to be opened.", connectionFactoryProxy.openedConnections() <= threads);
            assertTrue("It is expected " + threads + " Session to be opened and there are " + connectionFactoryProxy.openedSessions(), connectionFactoryProxy.openedSessions() <= threads);
            assertTrue("It is expected " + threads + " MessageProducer to be opened and there are " + connectionFactoryProxy.openedProducers(), connectionFactoryProxy.openedProducers() <= threads);
            assertTrue("Some resources were not closed.", connectionFactoryProxy.isAllResourcesClosed());
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    @Test
    public void whenExceptionIsRaisedDuringConnectionFactoryInitializationTheProcessorShouldBeYielded() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PublishJMS.class);

        // using JNDI JMS Connection Factory configured locally on the processor
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, "DummyInitialContextFactoryClass");
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, "DummyProviderUrl");
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, "DummyConnectionFactoryName");

        runner.setProperty(ConsumeJMS.DESTINATION, "myTopic");
        runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.TOPIC);

        try {
            runner.enqueue("message");
            runner.run();
            fail("The test was implemented in a way this line should not be reached.");
        } catch (AssertionError e) {
        } finally {
            assertTrue("In case of an exception, the processor should be yielded.", ((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        }
    }
}
