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
package org.apache.nifi.processors.kafka.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
public class ConsumeKafkaTest {

    @Test
    public void validateCustomSerilaizerDeserializerSettings() throws Exception {
        ConsumeKafka consumeKafka = new ConsumeKafka();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka.TOPIC, "foo");
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty("key.deserializer", "Foo");
        runner.assertNotValid();
    }

    @Test
    public void validatePropertiesValidation() throws Exception {
        ConsumeKafka consumeKafka = new ConsumeKafka();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka.TOPIC, "foo");
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafka.GROUP_ID);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("invalid because group.id is required"));
        }

        runner.setProperty(ConsumeKafka.GROUP_ID, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka.GROUP_ID, "  ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
    }

    /**
     * Will set auto-offset to 'smallest' to ensure that all events (the once
     * that were sent before and after consumer startup) are received.
     */
    @Test
    public void validateGetAllMessages() throws Exception {
        String topicName = "validateGetAllMessages";

        StubConsumeKafka consumeKafka = new StubConsumeKafka();

        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty("check.connection", "false");

        byte[][] values = new byte[][] { "Hello-1".getBytes(StandardCharsets.UTF_8),
                "Hello-2".getBytes(StandardCharsets.UTF_8), "Hello-3".getBytes(StandardCharsets.UTF_8) };
        consumeKafka.setValues(values);

        runner.run(1, false);

        values = new byte[][] { "Hello-4".getBytes(StandardCharsets.UTF_8), "Hello-5".getBytes(StandardCharsets.UTF_8),
                "Hello-6".getBytes(StandardCharsets.UTF_8) };
        consumeKafka.setValues(values);

        runner.run(1, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(6, flowFiles.size());
        // spot check
        MockFlowFile flowFile = flowFiles.get(0);
        String event = new String(flowFile.toByteArray());
        assertEquals("Hello-1", event);

        flowFile = flowFiles.get(1);
        event = new String(flowFile.toByteArray());
        assertEquals("Hello-2", event);

        flowFile = flowFiles.get(5);
        event = new String(flowFile.toByteArray());
        assertEquals("Hello-6", event);

        consumeKafka.close();
    }

    @Test
    public void validateGetAllMessagesWithProvidedDemarcator() throws Exception {
        String topicName = "validateGetAllMessagesWithProvidedDemarcator";

        StubConsumeKafka consumeKafka = new StubConsumeKafka();

        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "blah");
        runner.setProperty("check.connection", "false");

        byte[][] values = new byte[][] { "Hello-1".getBytes(StandardCharsets.UTF_8),
                "Hi-2".getBytes(StandardCharsets.UTF_8) };
        consumeKafka.setValues(values);

        runner.run(1, false);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        values = new byte[][] { "Здравствуйте-3".getBytes(StandardCharsets.UTF_8),
                "こんにちは-4".getBytes(StandardCharsets.UTF_8), "Hello-5".getBytes(StandardCharsets.UTF_8) };
        consumeKafka.setValues(values);

        runner.run(1, false);

        flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(2, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        String[] events = new String(flowFile.toByteArray(), StandardCharsets.UTF_8).split("blah");
        assertEquals("0", flowFile.getAttribute("kafka.partition"));
        assertEquals("0", flowFile.getAttribute("kafka.offset"));
        assertEquals("validateGetAllMessagesWithProvidedDemarcator", flowFile.getAttribute("kafka.topic"));

        assertEquals(2, events.length);

        flowFile = flowFiles.get(1);
        events = new String(flowFile.toByteArray(), StandardCharsets.UTF_8).split("blah");

        assertEquals(3, events.length);
        // spot check
        assertEquals("Здравствуйте-3", events[0]);
        assertEquals("こんにちは-4", events[1]);
        assertEquals("Hello-5", events[2]);

        consumeKafka.close();
    }
}
