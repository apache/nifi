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
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ConsumeKafkaTest {

    ConsumerLease mockLease = null;
    ConsumerPool mockConsumerPool = null;

    @BeforeEach
    public void setup() {
        mockLease = mock(ConsumerLease.class);
        mockConsumerPool = mock(ConsumerPool.class);
    }

    @Test
    public void validateCustomValidatorSettings() {
        ConsumeKafka_1_0 consumeKafka = new ConsumeKafka_1_0();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(ConsumeKafka_1_0.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_1_0.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_1_0.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_1_0.AUTO_OFFSET_RESET, ConsumeKafka_1_0.OFFSET_EARLIEST);
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        runner.assertValid();
    }

    @Test
    public void validatePropertiesValidation() {
        ConsumeKafka_1_0 consumeKafka = new ConsumeKafka_1_0();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(ConsumeKafka_1_0.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_1_0.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_1_0.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_1_0.AUTO_OFFSET_RESET, ConsumeKafka_1_0.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafka_1_0.GROUP_ID);

        AssertionError e = assertThrows(AssertionError.class, runner::assertValid);
        assertTrue(e.getMessage().contains("invalid because Group ID is required"));

        runner.setProperty(ConsumeKafka_1_0.GROUP_ID, "");

        e = assertThrows(AssertionError.class, runner::assertValid);
        assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));

        runner.setProperty(ConsumeKafka_1_0.GROUP_ID, "  ");

        e = assertThrows(AssertionError.class, runner::assertValid);
        assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
    }

    @Test
    public void testJaasConfiguration() {
        ConsumeKafka_1_0 consumeKafka = new ConsumeKafka_1_0();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(ConsumeKafka_1_0.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_1_0.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_1_0.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_1_0.AUTO_OFFSET_RESET, ConsumeKafka_1_0.OFFSET_EARLIEST);

        runner.setProperty(ConsumeKafka_1_0.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.assertNotValid();

        runner.setProperty(ConsumeKafka_1_0.KERBEROS_SERVICE_NAME, "kafka");
        runner.assertNotValid();

        runner.setProperty(ConsumeKafka_1_0.KERBEROS_PRINCIPAL, "nifi@APACHE.COM");
        runner.assertNotValid();

        runner.setProperty(ConsumeKafka_1_0.KERBEROS_KEYTAB, "not.A.File");
        runner.assertNotValid();

        runner.setProperty(ConsumeKafka_1_0.KERBEROS_KEYTAB, "src/test/resources/server.properties");
        runner.assertValid();

        runner.setVariable("keytab", "src/test/resources/server.properties");
        runner.setVariable("principal", "nifi@APACHE.COM");
        runner.setVariable("service", "kafka");
        runner.setProperty(ConsumeKafka_1_0.KERBEROS_PRINCIPAL, "${principal}");
        runner.setProperty(ConsumeKafka_1_0.KERBEROS_KEYTAB, "${keytab}");
        runner.setProperty(ConsumeKafka_1_0.KERBEROS_SERVICE_NAME, "${service}");
        runner.assertValid();
    }

}
