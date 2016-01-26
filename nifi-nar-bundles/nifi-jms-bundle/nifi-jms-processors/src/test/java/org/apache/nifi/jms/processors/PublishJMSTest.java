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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

public class PublishJMSTest {

    @Test
    public void validateSuccessfulPublishAndTransferToSuccess() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        PublishJMS pubProc = new PublishJMS();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(cf);

        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(PublishJMS.DESTINATION, "fooQueue");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        attributes.put(JmsHeaders.REPLY_TO, "cooQueue");
        runner.enqueue("Hey dude!".getBytes(), attributes);
        runner.run(1, false);

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
        assertNotNull(successFF);

        JmsTemplate jmst = new JmsTemplate(cf);
        jmst.setDefaultDestinationName("fooQueue");
        BytesMessage message = (BytesMessage) jmst.receive();

        byte[] messageBytes = MessageBodyToBytesConverter.toBytes(message);
        assertEquals("Hey dude!", new String(messageBytes));
        assertEquals("cooQueue", ((Queue) message.getJMSReplyTo()).getQueueName());
        assertEquals("foo", message.getStringProperty("foo"));
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
        runner.setProperty(PublishJMS.DESTINATION, "fooQueue");

        runner.enqueue("Hello Joe".getBytes());

        runner.run();
        Thread.sleep(200);

        assertTrue(runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).isEmpty());
        assertNotNull(runner.getFlowFilesForRelationship(PublishJMS.REL_FAILURE).get(0));
    }
}
