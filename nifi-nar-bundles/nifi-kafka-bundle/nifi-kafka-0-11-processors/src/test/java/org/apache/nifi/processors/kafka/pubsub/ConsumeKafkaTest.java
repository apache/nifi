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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ConsumeKafkaTest {

    ConsumerLease mockLease = null;
    ConsumerPool mockConsumerPool = null;

    @Before
    public void setup() {
        mockLease = mock(ConsumerLease.class);
        mockConsumerPool = mock(ConsumerPool.class);
    }

    @Test
    public void validateCustomValidatorSettings() throws Exception {
        ConsumeKafka_0_11 consumeKafka = new ConsumeKafka_0_11();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_0_11.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_0_11.AUTO_OFFSET_RESET, ConsumeKafka_0_11.OFFSET_EARLIEST);
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Foo");
        runner.assertNotValid();
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        runner.assertValid();
        runner.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        runner.assertNotValid();
    }

    @Test
    public void validatePropertiesValidation() throws Exception {
        ConsumeKafka_0_11 consumeKafka = new ConsumeKafka_0_11();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_0_11.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_0_11.AUTO_OFFSET_RESET, ConsumeKafka_0_11.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafka_0_11.GROUP_ID);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("invalid because Group ID is required"));
        }

        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, "  ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
    }

    @Test
    public void validateGetAllMessages() throws Exception {
        String groupName = "validateGetAllMessages";

        when(mockConsumerPool.obtainConsumer(anyObject(), anyObject())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(mockLease.commit()).thenReturn(Boolean.TRUE);

        ConsumeKafka_0_11 proc = new ConsumeKafka_0_11() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka_0_11.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka_0_11.AUTO_OFFSET_RESET, ConsumeKafka_0_11.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(anyObject(), anyObject());
        verify(mockLease, times(3)).continuePolling();
        verify(mockLease, times(2)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void validateGetAllMessagesPattern() throws Exception {
        String groupName = "validateGetAllMessagesPattern";

        when(mockConsumerPool.obtainConsumer(anyObject(), anyObject())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(mockLease.commit()).thenReturn(Boolean.TRUE);

        ConsumeKafka_0_11 proc = new ConsumeKafka_0_11() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka_0_11.TOPICS, "(fo.*)|(ba)");
        runner.setProperty(ConsumeKafka_0_11.TOPIC_TYPE, "pattern");
        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka_0_11.AUTO_OFFSET_RESET, ConsumeKafka_0_11.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(anyObject(), anyObject());
        verify(mockLease, times(3)).continuePolling();
        verify(mockLease, times(2)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void validateGetErrorMessages() throws Exception {
        String groupName = "validateGetErrorMessages";

        when(mockConsumerPool.obtainConsumer(anyObject(), anyObject())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(true, false);
        when(mockLease.commit()).thenReturn(Boolean.FALSE);

        ConsumeKafka_0_11 proc = new ConsumeKafka_0_11() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka_0_11.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka_0_11.AUTO_OFFSET_RESET, ConsumeKafka_0_11.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(anyObject(), anyObject());
        verify(mockLease, times(2)).continuePolling();
        verify(mockLease, times(1)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void testJaasConfiguration() throws Exception {
        ConsumeKafka_0_11 consumeKafka = new ConsumeKafka_0_11();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_0_11.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_0_11.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_0_11.AUTO_OFFSET_RESET, ConsumeKafka_0_11.OFFSET_EARLIEST);

        runner.setProperty(KafkaProcessorUtils.SECURITY_PROTOCOL, KafkaProcessorUtils.SEC_SASL_PLAINTEXT);
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.KERBEROS_PRINCIPLE, "kafka");
        runner.assertValid();

        runner.setProperty(KafkaProcessorUtils.USER_PRINCIPAL, "nifi@APACHE.COM");
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.USER_KEYTAB, "not.A.File");
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.USER_KEYTAB, "src/test/resources/server.properties");
        runner.assertValid();
    }

}
