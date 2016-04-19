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
import org.junit.Test;

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

    /**
     * Will set auto-offset to 'smallest' to ensure that all events (the once
     * that were sent before and after consumer startup) are received.
     */
    @Test
    public void testGetAllMessages() throws Exception {
        String topicName = "testGetAllMessages";

        ConsumeKafka getKafka = new ConsumeKafka();

        final TestRunner runner = TestRunners.newTestRunner(getKafka);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:" + kafkaLocal.getKafkaPort());
        runner.setProperty(ConsumeKafka.TOPIC, topicName);
        runner.setProperty(ConsumeKafka.CLIENT_ID, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
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

        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        String[] events = new String(flowFile.toByteArray()).split("\\s+");

        assertEquals(6, events.length);
        // spot check
        assertEquals("Hello-1", events[0]);
        assertEquals("Hello-4", events[3]);

        getKafka.shutdownConsumer();
    }

    /**
     * Based on auto-offset set to 'largest' events sent before consumer start
     * should not be consumed.
     *
     */
    @Test
    public void testGetOnlyMessagesAfterConsumerStartup() throws Exception {
        String topicName = "testGetOnlyMessagesAfterConsumerStartup";

        ConsumeKafka getKafka = new ConsumeKafka();
        final TestRunner runner = TestRunners.newTestRunner(getKafka);
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

        latch.await();

        runner.run(5, false);

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


        getKafka.shutdownConsumer();
    }
}
