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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

public class PublishKafkaTest {

    @Test
    public void validateCustomSerilaizerDeserializerSettings() throws Exception {
        PublishKafka_0_10 publishKafka = new PublishKafka_0_10();
        TestRunner runner = TestRunners.newTestRunner(publishKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(PublishKafka_0_10.TOPIC, "foo");
        runner.setProperty(PublishKafka_0_10.META_WAIT_TIME, "3 sec");
        runner.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        runner.assertValid();
        runner.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "Foo");
        runner.assertNotValid();
    }

    @Test
    public void validatePropertiesValidation() throws Exception {
        PublishKafka_0_10 publishKafka = new PublishKafka_0_10();
        TestRunner runner = TestRunners.newTestRunner(publishKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(PublishKafka_0_10.TOPIC, "foo");
        runner.setProperty(PublishKafka_0_10.META_WAIT_TIME, "foo");

        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("'max.block.ms' validated against 'foo' is invalid"));
        }
    }

    @Test
    public void validateCustomValidation() {
        String topicName = "validateCustomValidation";
        PublishKafka_0_10 publishKafka = new PublishKafka_0_10();

        /*
         * Validates that Kerberos principle is required if one of SASL set for
         * secirity protocol
         */
        TestRunner runner = TestRunners.newTestRunner(publishKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(KafkaProcessorUtils.SECURITY_PROTOCOL, KafkaProcessorUtils.SEC_SASL_PLAINTEXT);
        try {
            runner.run();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("'Kerberos Service Name' is invalid because"));
        }
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateSingleCharacterDemarcatedMessages() {
        String topicName = "validateSingleCharacterDemarcatedMessages";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "\n");

        runner.enqueue("Hello World\nGoodbye\n1\n2\n3\n4\n5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(7)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateMultiCharacterDemarcatedMessagesAndCustomPartitionerA() {
        String topicName = "validateMultiCharacterDemarcatedMessagesAndCustomPartitioner";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.PARTITION_CLASS, Partitioners.RoundRobinPartitioner.class.getName());
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "foo");

        runner.enqueue("Hello WorldfooGoodbyefoo1foo2foo3foo4foo5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(7)).send(Mockito.any(ProducerRecord.class));

        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateMultiCharacterDemarcatedMessagesAndCustomPartitionerB() {
        String topicName = "validateMultiCharacterDemarcatedMessagesAndCustomPartitioner";
        StubPublishKafka putKafka = new StubPublishKafka(1);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.PARTITION_CLASS, Partitioners.RoundRobinPartitioner.class.getName());
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "foo");

        runner.enqueue("Hello WorldfooGoodbyefoo1foo2foo3foo4foo5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(7)).send(Mockito.any(ProducerRecord.class));

        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateOnSendFailureAndThenResendSuccessA() throws Exception {
        String topicName = "validateSendFailureAndThenResendSuccess";
        StubPublishKafka putKafka = new StubPublishKafka(100);

        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(PublishKafka_0_10.META_WAIT_TIME, "3000 millis");

        final String text = "Hello World\nGoodbye\nfail\n2";
        runner.enqueue(text.getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        assertEquals(1, runner.getQueueSize().getObjectCount()); // due to failure
        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(4)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
        putKafka.destroy();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateOnSendFailureAndThenResendSuccessB() throws Exception {
        String topicName = "validateSendFailureAndThenResendSuccess";
        StubPublishKafka putKafka = new StubPublishKafka(1);

        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(PublishKafka_0_10.META_WAIT_TIME, "500 millis");

        final String text = "Hello World\nGoodbye\nfail\n2";
        runner.enqueue(text.getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        assertEquals(1, runner.getQueueSize().getObjectCount()); // due to failure
        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(4)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateOnFutureGetFailureAndThenResendSuccessFirstMessageFail() throws Exception {
        String topicName = "validateSendFailureAndThenResendSuccess";
        StubPublishKafka putKafka = new StubPublishKafka(100);

        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(PublishKafka_0_10.META_WAIT_TIME, "500 millis");

        final String text = "futurefail\nHello World\nGoodbye\n2";
        runner.enqueue(text.getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        MockFlowFile ff = runner.getFlowFilesForRelationship(PublishKafka_0_10.REL_FAILURE).get(0);
        assertNotNull(ff);
        runner.enqueue(ff);

        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        // 6 sends due to duplication
        verify(producer, times(5)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateOnFutureGetFailureAndThenResendSuccess() throws Exception {
        String topicName = "validateSendFailureAndThenResendSuccess";
        StubPublishKafka putKafka = new StubPublishKafka(100);

        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(PublishKafka_0_10.META_WAIT_TIME, "500 millis");

        final String text = "Hello World\nGoodbye\nfuturefail\n2";
        runner.enqueue(text.getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);
        MockFlowFile ff = runner.getFlowFilesForRelationship(PublishKafka_0_10.REL_FAILURE).get(0);
        assertNotNull(ff);
        runner.enqueue(ff);

        runner.run(1, false);
        assertEquals(0, runner.getQueueSize().getObjectCount());
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        // 6 sends due to duplication
        verify(producer, times(6)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateDemarcationIntoEmptyMessages() {
        String topicName = "validateDemarcationIntoEmptyMessages";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        final TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(PublishKafka_0_10.KEY, "key1");
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "\n");

        final byte[] bytes = "\n\n\n1\n2\n\n\n\n3\n4\n\n\n".getBytes(StandardCharsets.UTF_8);
        runner.enqueue(bytes);
        runner.run(1);
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(4)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateComplexRightPartialDemarcatedMessages() {
        String topicName = "validateComplexRightPartialDemarcatedMessages";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(3)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateComplexLeftPartialDemarcatedMessages() {
        String topicName = "validateComplexLeftPartialDemarcatedMessages";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>僠<僠WILDSTUFF僠>僠".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka_0_10.REL_SUCCESS, 1);
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(4)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void validateComplexPartialMatchDemarcatedMessages() {
        String topicName = "validateComplexPartialMatchDemarcatedMessages";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.MESSAGE_DEMARCATOR, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDBOOMSTUFF僠>僠".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka_0_10.REL_SUCCESS, 1);
        Producer<byte[], byte[]> producer = putKafka.getProducer();
        verify(producer, times(2)).send(Mockito.any(ProducerRecord.class));
        runner.shutdown();
    }

    @Test
    public void validateUtf8Key() {
        String topicName = "validateUtf8Key";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.KEY, "${myKey}");

        final Map<String, String> attributes = Collections.singletonMap("myKey", "key1");
        runner.enqueue("Hello World".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PublishKafka_0_10.REL_SUCCESS, 1);
        final Map<Object, Object> msgs = putKafka.getMessagesSent();
        assertEquals(1, msgs.size());
        final byte[] msgKey = (byte[]) msgs.keySet().iterator().next();
        assertTrue(Arrays.equals("key1".getBytes(StandardCharsets.UTF_8), msgKey));
    }

    @Test
    public void validateHexKey() {
        String topicName = "validateUtf8Key";
        StubPublishKafka putKafka = new StubPublishKafka(100);
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka_0_10.TOPIC, topicName);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "localhost:1234");
        runner.setProperty(PublishKafka_0_10.KEY_ATTRIBUTE_ENCODING, KafkaProcessorUtils.HEX_ENCODING);
        runner.setProperty(PublishKafka_0_10.KEY, "${myKey}");

        final Map<String, String> attributes = Collections.singletonMap("myKey", "6B657931");
        runner.enqueue("Hello World".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PublishKafka_0_10.REL_SUCCESS, 1);
        final Map<Object, Object> msgs = putKafka.getMessagesSent();
        assertEquals(1, msgs.size());
        final byte[] msgKey = (byte[]) msgs.keySet().iterator().next();

        assertTrue(Arrays.equals(new byte[] {0x6B, 0x65, 0x79, 0x31}, msgKey));
    }
}
