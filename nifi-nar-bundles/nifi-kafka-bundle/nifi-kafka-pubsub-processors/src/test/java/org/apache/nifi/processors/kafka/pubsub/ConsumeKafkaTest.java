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
import java.util.concurrent.CountDownLatch;

import org.apache.nifi.processors.kafka.pubsub.ConsumeKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafkaProducerHelper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
public class ConsumeKafkaTest {

    private static EmbeddedKafka kafkaLocal;

    private static EmbeddedKafkaProducerHelper producerHelper;

    @BeforeClass
    public static void bforeClass(){
        kafkaLocal = new EmbeddedKafka();
        kafkaLocal.start();
        producerHelper = new EmbeddedKafkaProducerHelper(kafkaLocal);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producerHelper.close();
        kafkaLocal.stop();
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

        ConsumeKafka consumeKafka = new ConsumeKafka();

        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:" + kafkaLocal.getKafkaPort());
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);

        producerHelper.sendEvent(topicName, "Hello-1");
        producerHelper.sendEvent(topicName, "Hello-2");
        producerHelper.sendEvent(topicName, "Hello-3");

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runner.run(10, false);
                } finally {
                    latch.countDown();
                }
            }
        }).start();

        producerHelper.sendEvent(topicName, "Hello-4");
        producerHelper.sendEvent(topicName, "Hello-5");
        producerHelper.sendEvent(topicName, "Hello-6");

        latch.await();

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

        ConsumeKafka consumeKafka = new ConsumeKafka();

        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:" + kafkaLocal.getKafkaPort());
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "blah");

        producerHelper.sendEvent(topicName, "Hello-1");
        producerHelper.sendEvent(topicName, "Hi-2");

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runner.run(10, false);
                } finally {
                    latch.countDown();
                }
            }
        }).start();

        producerHelper.sendEvent(topicName, "Здравствуйте-3");
        producerHelper.sendEvent(topicName, "こんにちは-4");
        producerHelper.sendEvent(topicName, "Hello-5");

        latch.await();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        String[] events = new String(flowFile.toByteArray(), StandardCharsets.UTF_8).split("blah");

        assertEquals(5, events.length);
        // spot check
        assertEquals("Здравствуйте-3", events[2]);
        assertEquals("こんにちは-4", events[3]);
        assertEquals("Hello-5", events[4]);

        consumeKafka.close();
    }

    /**
     * Based on auto-offset set to 'largest' events sent before consumer start
     * should not be consumed.
     *
     */
    @Test
    public void validateGetOnlyMessagesAfterConsumerStartup() throws Exception {
        String topicName = "validateGetOnlyMessagesAfterConsumerStartup";

        ConsumeKafka consumeKafka = new ConsumeKafka();
        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:" + kafkaLocal.getKafkaPort());
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");

        producerHelper.sendEvent(topicName, "Hello-1");
        producerHelper.sendEvent(topicName, "Hello-2");
        producerHelper.sendEvent(topicName, "Hello-3");

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runner.run(40, false);
                } finally {
                    latch.countDown();
                }
            }
        }).start();

        latch.await();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        producerHelper.sendEvent(topicName, "Hello-4");
        producerHelper.sendEvent(topicName, "Hello-5");
        producerHelper.sendEvent(topicName, "Hello-6");

        Thread.sleep(500);
        runner.run(5, false);
        Thread.sleep(200);
        flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(3, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        String event = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("Hello-4", event);

        flowFile = flowFiles.get(1);
        event = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("Hello-5", event);

        flowFile = flowFiles.get(2);
        event = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("Hello-6", event);

        consumeKafka.close();
    }

    @Test
    public void validateGetOnlyMessagesAfterConsumerStartupWithDemarcator() throws Exception {
        String topicName = "validateGetOnlyMessagesAfterConsumerStartupWithDemarcator";

        ConsumeKafka consumeKafka = new ConsumeKafka();
        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:" + kafkaLocal.getKafkaPort());
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "\n");

        producerHelper.sendEvent(topicName, "Hello-1");
        producerHelper.sendEvent(topicName, "Hello-2");
        producerHelper.sendEvent(topicName, "Hello-3");

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runner.run(40, false);
                } finally {
                    latch.countDown();
                }
            }
        }).start();

        latch.await();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        producerHelper.sendEvent(topicName, "Hello-4");
        producerHelper.sendEvent(topicName, "Hello-5");
        producerHelper.sendEvent(topicName, "Hello-6");

        runner.run(40, false);

        flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(2, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        String[] events = new String(flowFile.toByteArray()).split("\\s+");
        assertEquals(1, events.length);
        assertEquals("Hello-4", events[0]);

        flowFile = flowFiles.get(1);
        events = new String(flowFile.toByteArray()).split("\\s+");
        assertEquals(2, events.length);

        assertEquals("Hello-5", events[0]);
        assertEquals("Hello-6", events[1]);

        consumeKafka.close();
    }
}
