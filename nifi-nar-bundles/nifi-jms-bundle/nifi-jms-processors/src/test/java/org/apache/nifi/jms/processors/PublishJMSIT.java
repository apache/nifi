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
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
}
