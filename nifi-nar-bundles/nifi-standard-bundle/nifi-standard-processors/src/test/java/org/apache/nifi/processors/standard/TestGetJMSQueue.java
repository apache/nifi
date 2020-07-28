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
package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertTrue;

import java.util.List;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.util.JmsFactory;
import org.apache.nifi.processors.standard.util.JmsProperties;
import org.apache.nifi.processors.standard.util.WrappedMessageProducer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.Revision;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestGetJMSQueue {

    @Test
    public void testSendTextToQueue() throws Exception {
        PutJMS putJms = new PutJMS();
        TestRunner putRunner = TestRunners.newTestRunner(putJms);
        putRunner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        putRunner.setProperty(JmsProperties.URL, "vm://localhost?broker.persistent=false");
        putRunner.setProperty(JmsProperties.DESTINATION_TYPE, JmsProperties.DESTINATION_TYPE_QUEUE);
        putRunner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        putRunner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);

        WrappedMessageProducer wrappedProducer = JmsFactory.createMessageProducer(putRunner.getProcessContext(), true);
        final Session jmsSession = wrappedProducer.getSession();
        final MessageProducer producer = wrappedProducer.getProducer();
        final Message message = jmsSession.createTextMessage("Hello World");

        producer.send(message);
        jmsSession.commit();

        GetJMSQueue getJmsQueue = new GetJMSQueue();
        TestRunner runner = TestRunners.newTestRunner(getJmsQueue);
        runner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        runner.setProperty(JmsProperties.URL, "vm://localhost?broker.persistent=false");
        runner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        runner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);

        runner.run();

        List<MockFlowFile> flowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("success").build());

        assertTrue(flowFiles.size() == 1);
        MockFlowFile successFlowFile = flowFiles.get(0);
        successFlowFile.assertContentEquals("Hello World");
        successFlowFile.assertAttributeEquals("jms.JMSDestination", "queue.testing");
        producer.close();
        jmsSession.close();
    }

    @Test
    public void testSendBytesToQueue() throws Exception {
        PutJMS putJms = new PutJMS();
        TestRunner putRunner = TestRunners.newTestRunner(putJms);
        putRunner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        putRunner.setProperty(JmsProperties.URL, "vm://localhost?broker.persistent=false");
        putRunner.setProperty(JmsProperties.DESTINATION_TYPE, JmsProperties.DESTINATION_TYPE_QUEUE);
        putRunner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        putRunner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);
        WrappedMessageProducer wrappedProducer = JmsFactory.createMessageProducer(putRunner.getProcessContext(), true);
        final Session jmsSession = wrappedProducer.getSession();
        final MessageProducer producer = wrappedProducer.getProducer();
        final BytesMessage message = jmsSession.createBytesMessage();
        message.writeBytes("Hello Bytes".getBytes());

        producer.send(message);
        jmsSession.commit();

        GetJMSQueue getJmsQueue = new GetJMSQueue();
        TestRunner runner = TestRunners.newTestRunner(getJmsQueue);
        runner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        runner.setProperty(JmsProperties.URL, "vm://localhost?broker.persistent=false");
        runner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        runner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);

        runner.run();

        List<MockFlowFile> flowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("success").build());

        assertTrue(flowFiles.size() == 1);
        MockFlowFile successFlowFile = flowFiles.get(0);
        successFlowFile.assertContentEquals("Hello Bytes");
        successFlowFile.assertAttributeEquals("jms.JMSDestination", "queue.testing");
        producer.close();
        jmsSession.close();
    }

    @Test
    public void testSendStreamToQueue() throws Exception {
        PutJMS putJms = new PutJMS();
        TestRunner putRunner = TestRunners.newTestRunner(putJms);
        putRunner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        putRunner.setProperty(JmsProperties.URL, "vm://localhost?broker.persistent=false");
        putRunner.setProperty(JmsProperties.DESTINATION_TYPE, JmsProperties.DESTINATION_TYPE_QUEUE);
        putRunner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        putRunner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);
        WrappedMessageProducer wrappedProducer = JmsFactory.createMessageProducer(putRunner.getProcessContext(), true);
        final Session jmsSession = wrappedProducer.getSession();
        final MessageProducer producer = wrappedProducer.getProducer();

        final StreamMessage message = jmsSession.createStreamMessage();
        message.writeBytes("Hello Stream".getBytes());

        producer.send(message);
        jmsSession.commit();

        GetJMSQueue getJmsQueue = new GetJMSQueue();
        TestRunner runner = TestRunners.newTestRunner(getJmsQueue);
        runner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        runner.setProperty(JmsProperties.URL, "vm://localhost?broker.persistent=false");
        runner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        runner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);

        runner.run();

        List<MockFlowFile> flowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("success").build());

        assertTrue(flowFiles.size() == 1);
        MockFlowFile successFlowFile = flowFiles.get(0);
        successFlowFile.assertContentEquals("Hello Stream");
        successFlowFile.assertAttributeEquals("jms.JMSDestination", "queue.testing");

        producer.close();
        jmsSession.close();
    }

    @org.junit.Ignore
    public void testSendMapToQueue() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(GetJMSQueue.class);
        runner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        runner.setProperty(JmsProperties.URL, "tcp://localhost:61616");
        runner.setProperty(JmsProperties.DESTINATION_TYPE, JmsProperties.DESTINATION_TYPE_QUEUE);
        runner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        runner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);
        WrappedMessageProducer wrappedProducer = JmsFactory.createMessageProducer(runner.getProcessContext(), true);
        final Session jmsSession = wrappedProducer.getSession();
        final MessageProducer producer = wrappedProducer.getProducer();

        final MapMessage message = jmsSession.createMapMessage();
        message.setString("foo!", "bar");
        message.setString("bacon", "meat");

        producer.send(message);
        jmsSession.commit();
        producer.close();
        jmsSession.close();
    }

    @org.junit.Ignore
    public void testSendObjectToQueue() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(GetJMSQueue.class);
        runner.setProperty(JmsProperties.JMS_PROVIDER, JmsProperties.ACTIVEMQ_PROVIDER);
        runner.setProperty(JmsProperties.URL, "tcp://localhost:61616");
        runner.setProperty(JmsProperties.DESTINATION_TYPE, JmsProperties.DESTINATION_TYPE_QUEUE);
        runner.setProperty(JmsProperties.DESTINATION_NAME, "queue.testing");
        runner.setProperty(JmsProperties.ACKNOWLEDGEMENT_MODE, JmsProperties.ACK_MODE_AUTO);
        WrappedMessageProducer wrappedProducer = JmsFactory.createMessageProducer(runner.getProcessContext(), true);
        final Session jmsSession = wrappedProducer.getSession();
        final MessageProducer producer = wrappedProducer.getProducer();

        // Revision class is used because test just needs any Serializable class in core NiFi
        final ObjectMessage message = jmsSession.createObjectMessage(new Revision(1L, "ID", "COMP_ID"));

        producer.send(message);
        jmsSession.commit();
        producer.close();
        jmsSession.close();
    }
}
