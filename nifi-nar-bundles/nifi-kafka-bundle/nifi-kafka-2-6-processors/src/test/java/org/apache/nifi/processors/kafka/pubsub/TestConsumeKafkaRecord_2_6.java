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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordParser;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestConsumeKafkaRecord_2_6 {

    private ConsumerLease mockLease = null;
    private ConsumerPool mockConsumerPool = null;
    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException {
        mockLease = mock(ConsumerLease.class);
        mockConsumerPool = mock(ConsumerPool.class);

        ConsumeKafkaRecord_2_6 proc = new ConsumeKafkaRecord_2_6() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };

        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ConsumeKafkaRecord_2_6.BOOTSTRAP_SERVERS, "okeydokey:1234");

        final String readerId = "record-reader";
        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);

        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new MockRecordWriter("name, age");
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConsumeKafkaRecord_2_6.RECORD_READER, readerId);
        runner.setProperty(ConsumeKafkaRecord_2_6.RECORD_WRITER, writerId);
    }

    @Test
    public void validateCustomValidatorSettings() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        runner.assertValid();
    }

    @Test
    public void validatePropertiesValidation() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafkaRecord_2_6.GROUP_ID);

        AssertionError e = assertThrows(AssertionError.class, () -> runner.assertValid());
        assertTrue(e.getMessage().contains("invalid because Group ID is required"));

        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "");

        e = assertThrows(AssertionError.class, () -> runner.assertValid());
        assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));

        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "  ");

        e = assertThrows(AssertionError.class, () -> runner.assertValid());
        assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
    }

    @Test
    public void validateGetAllMessages() {
        String groupName = "validateGetAllMessages";

        when(mockConsumerPool.obtainConsumer(any(), any())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(mockLease.commit()).thenReturn(Boolean.TRUE);

        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(any(), any());
        verify(mockLease, times(3)).continuePolling();
        verify(mockLease, times(2)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void validateGetAllMessagesPattern() {
        String groupName = "validateGetAllMessagesPattern";

        when(mockConsumerPool.obtainConsumer(any(), any())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(mockLease.commit()).thenReturn(Boolean.TRUE);

        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "(fo.*)|(ba)");
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPIC_TYPE, "pattern");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(any(), any());
        verify(mockLease, times(3)).continuePolling();
        verify(mockLease, times(2)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void validateGetErrorMessages() {
        String groupName = "validateGetErrorMessages";

        when(mockConsumerPool.obtainConsumer(any(), any())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(true, false);
        when(mockLease.commit()).thenReturn(Boolean.FALSE);

        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(any(), any());
        verify(mockLease, times(2)).continuePolling();
        verify(mockLease, times(1)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void testJaasConfigurationWithDefaultMechanism() throws InitializationException {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);

        runner.setProperty(ConsumeKafkaRecord_2_6.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.KERBEROS_SERVICE_NAME, "kafka");
        runner.assertNotValid();

        final KerberosUserService kerberosUserService = enableKerberosUserService(runner);
        runner.setProperty(ConsumeKafka_2_6.SELF_CONTAINED_KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertValid();
    }

    @Test
    public void testJaasConfigurationWithPlainMechanism() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);

        runner.setProperty(ConsumeKafkaRecord_2_6.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_MECHANISM, SaslMechanism.PLAIN);
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_USERNAME, "user1");
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_PASSWORD, "password");
        runner.assertValid();

        runner.removeProperty(ConsumeKafkaRecord_2_6.SASL_USERNAME);
        runner.assertNotValid();
    }

    @Test
    public void testJaasConfigurationWithScram256Mechanism() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);

        runner.setProperty(ConsumeKafkaRecord_2_6.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_MECHANISM, SaslMechanism.SCRAM_SHA_256);
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_USERNAME, "user1");
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_PASSWORD, "password");
        runner.assertValid();

        runner.removeProperty(ConsumeKafkaRecord_2_6.SASL_USERNAME);
        runner.assertNotValid();
    }

    @Test
    public void testJaasConfigurationWithScram512Mechanism() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);

        runner.setProperty(ConsumeKafkaRecord_2_6.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_MECHANISM, SaslMechanism.SCRAM_SHA_512);
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_USERNAME, "user1");
        runner.assertNotValid();

        runner.setProperty(ConsumeKafkaRecord_2_6.SASL_PASSWORD, "password");
        runner.assertValid();

        runner.removeProperty(ConsumeKafkaRecord_2_6.SASL_USERNAME);
        runner.assertNotValid();
    }

    @Test
    public void testNonSaslSecurityProtocol() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.AUTO_OFFSET_RESET, ConsumeKafkaRecord_2_6.OFFSET_EARLIEST);

        runner.setProperty(ConsumeKafkaRecord_2_6.SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name());
        runner.assertValid();
    }

    private SelfContainedKerberosUserService enableKerberosUserService(final TestRunner runner) throws InitializationException {
        final SelfContainedKerberosUserService kerberosUserService = mock(SelfContainedKerberosUserService.class);
        when(kerberosUserService.getIdentifier()).thenReturn("userService1");
        runner.addControllerService(kerberosUserService.getIdentifier(), kerberosUserService);
        runner.enableControllerService(kerberosUserService);
        return kerberosUserService;
    }

}
