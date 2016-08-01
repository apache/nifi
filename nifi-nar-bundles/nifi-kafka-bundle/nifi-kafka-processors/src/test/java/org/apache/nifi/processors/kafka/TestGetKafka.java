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
package org.apache.nifi.processors.kafka;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.log4j.BasicConfigurator;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestGetKafka {

    @BeforeClass
    public static void configureLogging() {
        System.setProperty("org.slf4j.simpleLogger.log.kafka", "INFO");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.kafka", "INFO");
        BasicConfigurator.configure();
    }

    @Test
    @Ignore("Intended only for local tests to verify functionality.")
    public void testIntegrationLocally() {
        final TestRunner runner = TestRunners.newTestRunner(GetKafka.class);
        runner.setProperty(GetKafka.ZOOKEEPER_CONNECTION_STRING, "192.168.0.101:2181");
        runner.setProperty(GetKafka.TOPIC, "testX");
        runner.setProperty(GetKafka.KAFKA_TIMEOUT, "3 secs");
        runner.setProperty(GetKafka.ZOOKEEPER_TIMEOUT, "3 secs");

        runner.run(20, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetKafka.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            System.out.println(flowFile.getAttributes());
            System.out.println(new String(flowFile.toByteArray()));
            System.out.println();
        }
    }

    @Test
    public void testWithDelimiter() {
        final List<String> messages = new ArrayList<>();
        messages.add("Hello");
        messages.add("Good-bye");

        final TestableProcessor proc = new TestableProcessor(null, messages);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(GetKafka.ZOOKEEPER_CONNECTION_STRING, "localhost:2181");
        runner.setProperty(GetKafka.TOPIC, "testX");
        runner.setProperty(GetKafka.KAFKA_TIMEOUT, "3 secs");
        runner.setProperty(GetKafka.ZOOKEEPER_TIMEOUT, "3 secs");
        runner.setProperty(GetKafka.MESSAGE_DEMARCATOR, "\\n");
        runner.setProperty(GetKafka.BATCH_SIZE, "2");

        runner.run();

        runner.assertAllFlowFilesTransferred(GetKafka.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetKafka.REL_SUCCESS).get(0);
        mff.assertContentEquals("Hello\nGood-bye");
    }

    @Test
    public void testWithDelimiterAndNotEnoughMessages() {
        final List<String> messages = new ArrayList<>();
        messages.add("Hello");
        messages.add("Good-bye");

        final TestableProcessor proc = new TestableProcessor(null, messages);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(GetKafka.ZOOKEEPER_CONNECTION_STRING, "localhost:2181");
        runner.setProperty(GetKafka.TOPIC, "testX");
        runner.setProperty(GetKafka.KAFKA_TIMEOUT, "3 secs");
        runner.setProperty(GetKafka.ZOOKEEPER_TIMEOUT, "3 secs");
        runner.setProperty(GetKafka.MESSAGE_DEMARCATOR, "\\n");
        runner.setProperty(GetKafka.BATCH_SIZE, "3");

        runner.run();

        runner.assertAllFlowFilesTransferred(GetKafka.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetKafka.REL_SUCCESS).get(0);
        mff.assertContentEquals("Hello\nGood-bye");
    }

    private static class TestableProcessor extends GetKafka {

        private final byte[] key;
        private final Iterator<String> messageItr;

        public TestableProcessor(final byte[] key, final List<String> messages) {
            this.key = key;
            messageItr = messages.iterator();
        }

        @Override
        public void createConsumers(ProcessContext context) {
            try {
                Field f = GetKafka.class.getDeclaredField("consumerStreamsReady");
                f.setAccessible(true);
                ((AtomicBoolean) f.get(this)).set(true);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        protected ConsumerIterator<byte[], byte[]> getStreamIterator() {
            final ConsumerIterator<byte[], byte[]> itr = Mockito.mock(ConsumerIterator.class);

            Mockito.doAnswer(new Answer<Boolean>() {
                @Override
                public Boolean answer(final InvocationOnMock invocation) throws Throwable {
                    return messageItr.hasNext();
                }
            }).when(itr).hasNext();

            Mockito.doAnswer(new Answer<MessageAndMetadata>() {
                @Override
                public MessageAndMetadata answer(InvocationOnMock invocation) throws Throwable {
                    final MessageAndMetadata mam = Mockito.mock(MessageAndMetadata.class);
                    Mockito.when(mam.key()).thenReturn(key);
                    Mockito.when(mam.offset()).thenReturn(0L);
                    Mockito.when(mam.partition()).thenReturn(0);

                    Mockito.doAnswer(new Answer<byte[]>() {
                        @Override
                        public byte[] answer(InvocationOnMock invocation) throws Throwable {
                            return messageItr.next().getBytes();
                        }

                    }).when(mam).message();

                    return mam;
                }
            }).when(itr).next();

            return itr;
        }
    }

    @Test
    public void testValidateExternalStateAccess() throws Exception {
        final GetKafka processor = new GetKafka();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        Collection<ValidationResult> validationResults = processor.validateExternalStateAccess(runner.newValidationContext());
        // Group Id has default value. These two properties are required.
        assertEquals(2, validationResults.size());
        final List<String> explanations = validationResults.stream().map(r -> r.getExplanation()).collect(Collectors.toList());
        assertTrue(explanations.contains("Topic Name is required"));
        assertTrue(explanations.contains("ZooKeeper Connection String is required"));

        // Set required properties, validation passes, values are set
        runner.setProperty(GetKafka.ZOOKEEPER_CONNECTION_STRING, "0.0.0.0:9092");
        runner.setProperty(GetKafka.TOPIC, "testX");

        validationResults = processor.validateExternalStateAccess(runner.newValidationContext());
        assertEquals(0, validationResults.size());

        assertEquals("testX", getPrivateFieldValue(processor, "topic"));
        assertEquals("0.0.0.0:9092", getPrivateFieldValue(processor, "zookeeperConnectionString"));
        assertNotNull("Default groupId should be used", getPrivateFieldValue(processor, "groupId"));

        // Set groupId
        runner.setProperty(GetKafka.GROUP_ID, "consumer-group-id");

        validationResults = processor.validateExternalStateAccess(runner.newValidationContext());
        assertEquals(0, validationResults.size());

        assertEquals("testX", getPrivateFieldValue(processor, "topic"));
        assertEquals("0.0.0.0:9092", getPrivateFieldValue(processor, "zookeeperConnectionString"));
        assertNotNull("consumer-group-id", getPrivateFieldValue(processor, "groupId"));
    }

    private Object getPrivateFieldValue(GetKafka processor, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        final Field field = processor.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(processor);
    }

}
