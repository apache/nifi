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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.mockito.Mockito;

// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
public class ConsumeKafkaTest {

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

    @Test
    public void testGetState() throws Exception {

        final ConsumeKafka processor = new ConsumeKafka();
        final ConsumeKafka consumeKafka = Mockito.spy(processor);
        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);

        final String topicEL = "${literal('topic'):toUpper()}";
        final String topic = "TOPIC";

        runner.setProperty(ConsumeKafka.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPIC, topicEL);
        runner.setProperty(ConsumeKafka.GROUP_ID, "GroupId");
        runner.setProperty(processor.CLIENT_ID, "ClientId");

        Collection<ValidationResult> validationResults = processor.validateExternalStateAccess(runner.newValidationContext());
        assertEquals(0, validationResults.size());

        assertEquals(topic, getPrivateFieldValue(processor, "topic"));
        assertEquals("0.0.0.0:1234", getPrivateFieldValue(processor, "brokers"));

        final Consumer consumer = mock(Consumer.class);
        doReturn(consumer).when(consumeKafka).buildKafkaResource(eq("-temp-command"));

        final List<PartitionInfo> partitionInfo = IntStream.range(0, 3).mapToObj(p -> new PartitionInfo(topic, p, null, null, null))
                .collect(Collectors.toList());
        doReturn(partitionInfo).when(consumer).partitionsFor(any());

        doReturn(new OffsetAndMetadata(100))
                .doReturn(new OffsetAndMetadata(101))
                .doReturn(new OffsetAndMetadata(102))
                .when(consumer).committed(any(TopicPartition.class));

        final StateMap state = consumeKafka.getExternalState();

        assertEquals(3, state.toMap().size());
        assertEquals(state.get("partition:0"), "100");
        assertEquals(state.get("partition:1"), "101");
        assertEquals(state.get("partition:2"), "102");
    }

    @Test
    public void testClearState() throws Exception {

        System.setProperty("org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaTest.bootstrapServers", "set-from-env:9092");

        final ConsumeKafka processor = new ConsumeKafka();
        final ConsumeKafka consumeKafka = Mockito.spy(processor);
        final TestRunner runner = TestRunners.newTestRunner(consumeKafka);

        // Without required properties.
        Collection<ValidationResult> validationResults = processor.validateExternalStateAccess(runner.newValidationContext());
        assertEquals(3, validationResults.size());
        final List<String> explanations = validationResults.stream().map(r -> r.getExplanation()).collect(Collectors.toList());
        assertTrue(explanations.contains("topic is required"));
        assertTrue(explanations.contains("client.id is required"));
        assertTrue(explanations.contains("group.id is required"));

        // brokers has default value.
        assertNull(getPrivateFieldValue(processor, "topic"));
        assertEquals("localhost:9092", getPrivateFieldValue(processor, "brokers"));

        // Set required properties
        final String topic = "topic";
        runner.setProperty(processor.BOOTSTRAP_SERVERS, "${org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaTest.bootstrapServers}");
        runner.setProperty(processor.TOPIC, topic);
        runner.setProperty(processor.GROUP_ID, "GroupId");
        runner.setProperty(processor.CLIENT_ID, "ClientId");

        validationResults = processor.validateExternalStateAccess(runner.newValidationContext());
        assertEquals(0, validationResults.size());

        assertEquals(topic, getPrivateFieldValue(processor, "topic"));
        assertEquals("set-from-env:9092", getPrivateFieldValue(processor, "brokers"));

        final Consumer consumer = mock(Consumer.class);
        doReturn(consumer).when(consumeKafka).buildKafkaResource(eq("-temp-command"));

        final List<PartitionInfo> partitionInfo = IntStream.range(0, 3).mapToObj(p -> new PartitionInfo(topic, p, null, null, null))
                .collect(Collectors.toList());
        doReturn(partitionInfo).when(consumer).partitionsFor(any());

        doAnswer(invocation -> {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = (Map<TopicPartition, OffsetAndMetadata>)invocation.getArguments()[0];
            assertEquals(3, committedOffsets.size());
            committedOffsets.values().stream().forEach(offset -> assertEquals("Offset should be cleared with -1", -1, offset.offset()));
            return null;
        }).when(consumer).commitSync(any());

        consumeKafka.clearExternalState();

        verify(consumer).commitSync(any());
    }

    private Object getPrivateFieldValue(ConsumeKafka processor, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        final Field field = processor.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(processor);
    }
}
