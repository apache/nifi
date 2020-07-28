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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.TextMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.standard.util.JmsProcessingSummary;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("deprecation")
public class TestJmsConsumer {

    static protected MapMessage createMapMessage() throws JMSException {
        MapMessage mapMessage = new ActiveMQMapMessage();
        mapMessage.setString("name", "Arnold");
        mapMessage.setInt("age", 97);
        mapMessage.setDouble("xyz", 89686.564);
        mapMessage.setBoolean("good", true);
        return mapMessage;
    }

    /**
     * Test method for {@link org.apache.nifi.processors.standard.JmsConsumer#createMapMessageAttrs(javax.jms.MapMessage)}.
     *
     * @throws JMSException jms
     */
    @Test
    public void testCreateMapMessageValues() throws JMSException {

        MapMessage mapMessage = createMapMessage();

        Map<String, String> mapMessageValues = JmsConsumer.createMapMessageValues(mapMessage);
        assertEquals("", 4, mapMessageValues.size());
        assertEquals("", "Arnold", mapMessageValues.get(JmsConsumer.MAP_MESSAGE_PREFIX + "name"));
        assertEquals("", "97", mapMessageValues.get(JmsConsumer.MAP_MESSAGE_PREFIX + "age"));
        assertEquals("", "89686.564", mapMessageValues.get(JmsConsumer.MAP_MESSAGE_PREFIX + "xyz"));
        assertEquals("", "true", mapMessageValues.get(JmsConsumer.MAP_MESSAGE_PREFIX + "good"));
    }

    /**
     * Test MapMessage to FlowFile conversion
     *
     * @throws java.lang.Exception ex
     */
    @Test
    public void testMap2FlowFileMapMessage() throws Exception {

        TestRunner runner = TestRunners.newTestRunner(GetJMSQueue.class);
        MapMessage mapMessage = createMapMessage();

        ProcessContext context = runner.getProcessContext();
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        ProcessorInitializationContext pic = new MockProcessorInitializationContext(runner.getProcessor(), (MockProcessContext) runner.getProcessContext());

        JmsProcessingSummary summary = JmsConsumer.map2FlowFile(context, session, mapMessage, true, pic.getLogger());

        assertEquals("MapMessage should not create FlowFile content", 0, summary.getBytesReceived());

        Map<String, String> attributes = summary.getLastFlowFile().getAttributes();
        assertEquals("", "Arnold", attributes.get(JmsConsumer.MAP_MESSAGE_PREFIX + "name"));
        assertEquals("", "97", attributes.get(JmsConsumer.MAP_MESSAGE_PREFIX + "age"));
        assertEquals("", "89686.564", attributes.get(JmsConsumer.MAP_MESSAGE_PREFIX + "xyz"));
        assertEquals("", "true", attributes.get(JmsConsumer.MAP_MESSAGE_PREFIX + "good"));
    }

    @Test
    public void testMap2FlowFileTextMessage() throws Exception {

        TestRunner runner = TestRunners.newTestRunner(GetJMSQueue.class);
        TextMessage textMessage = new ActiveMQTextMessage();

        String payload = "Hello world!";
        textMessage.setText(payload);

        ProcessContext context = runner.getProcessContext();
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        ProcessorInitializationContext pic = new MockProcessorInitializationContext(runner.getProcessor(), (MockProcessContext) runner.getProcessContext());

        JmsProcessingSummary summary = JmsConsumer.map2FlowFile(context, session, textMessage, true, pic.getLogger());

        assertEquals("TextMessage content length should equal to FlowFile content size", payload.length(), summary.getLastFlowFile().getSize());

        final byte[] buffer = new byte[payload.length()];
        runner.clearTransferState();

        session.read(summary.getLastFlowFile(), new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer, false);
            }
        });

        String contentString = new String(buffer, "UTF-8");
        assertEquals("", payload, contentString);
    }

    /**
     * Test BytesMessage to FlowFile conversion
     *
     * @throws java.lang.Exception ex
     */
    @Test
    public void testMap2FlowFileBytesMessage() throws Exception {

        TestRunner runner = TestRunners.newTestRunner(GetJMSQueue.class);
        BytesMessage bytesMessage = new ActiveMQBytesMessage();

        String sourceString = "Apache NiFi is an easy to use, powerful, and reliable system to process and distribute data.!";
        byte[] payload = sourceString.getBytes("UTF-8");
        bytesMessage.writeBytes(payload);
        bytesMessage.reset();

        ProcessContext context = runner.getProcessContext();
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        ProcessorInitializationContext pic = new MockProcessorInitializationContext(runner.getProcessor(), (MockProcessContext) runner.getProcessContext());

        JmsProcessingSummary summary = JmsConsumer.map2FlowFile(context, session, bytesMessage, true, pic.getLogger());

        assertEquals("BytesMessage content length should equal to FlowFile content size", payload.length, summary.getLastFlowFile().getSize());

        final byte[] buffer = new byte[payload.length];
        runner.clearTransferState();

        session.read(summary.getLastFlowFile(), new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer, false);
            }
        });

        String contentString = new String(buffer, "UTF-8");
        assertEquals("", sourceString, contentString);
    }

}
