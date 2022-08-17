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
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConsumeKafka_2_6 {

    ConsumerLease mockLease = null;
    ConsumerPool mockConsumerPool = null;

    @BeforeEach
    public void setup() {
        mockLease = mock(ConsumerLease.class);
        mockConsumerPool = mock(ConsumerPool.class);
    }

    @Test
    public void validateCustomValidatorSettings() throws Exception {
        ConsumeKafka_2_6 consumeKafka = new ConsumeKafka_2_6();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_2_6.AUTO_OFFSET_RESET, ConsumeKafka_2_6.OFFSET_EARLIEST);
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
        ConsumeKafka_2_6 consumeKafka = new ConsumeKafka_2_6();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_2_6.AUTO_OFFSET_RESET, ConsumeKafka_2_6.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafka_2_6.GROUP_ID);

        AssertionError e = assertThrows(AssertionError.class, () -> runner.assertValid());
        assertTrue(e.getMessage().contains("invalid because Group ID is required"));

        runner.setProperty(ConsumeKafka_2_6.GROUP_ID, "");

        e = assertThrows(AssertionError.class, () -> runner.assertValid());
        assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));

        runner.setProperty(ConsumeKafka_2_6.GROUP_ID, "  ");

        e = assertThrows(AssertionError.class, () -> runner.assertValid());
        assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
    }

    @Test
    public void testJaasGssApiConfiguration() throws Exception {
        ConsumeKafka_2_6 consumeKafka = new ConsumeKafka_2_6();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_2_6.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_2_6.AUTO_OFFSET_RESET, ConsumeKafka_2_6.OFFSET_EARLIEST);

        runner.setProperty(KafkaProcessorUtils.SECURITY_PROTOCOL, KafkaProcessorUtils.SEC_SASL_PLAINTEXT);
        runner.setProperty(KafkaProcessorUtils.SASL_MECHANISM, KafkaProcessorUtils.GSSAPI_VALUE);
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.JAAS_SERVICE_NAME, "kafka");
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.USER_PRINCIPAL, "nifi@APACHE.COM");
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.USER_KEYTAB, "not.A.File");
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.USER_KEYTAB, "src/test/resources/server.properties");
        runner.assertValid();

        runner.setVariable("keytab", "src/test/resources/server.properties");
        runner.setVariable("principal", "nifi@APACHE.COM");
        runner.setVariable("service", "kafka");
        runner.setProperty(KafkaProcessorUtils.USER_PRINCIPAL, "${principal}");
        runner.setProperty(KafkaProcessorUtils.USER_KEYTAB, "${keytab}");
        runner.setProperty(KafkaProcessorUtils.JAAS_SERVICE_NAME, "${service}");
        runner.assertValid();

        final KerberosUserService kerberosUserService = enableKerberosUserService(runner);
        runner.setProperty(KafkaProcessorUtils.SELF_CONTAINED_KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertNotValid();

        runner.removeProperty(KafkaProcessorUtils.USER_PRINCIPAL);
        runner.removeProperty(KafkaProcessorUtils.USER_KEYTAB);
        runner.assertValid();

        final KerberosCredentialsService kerberosCredentialsService = enabledKerberosCredentialsService(runner);
        runner.setProperty(KafkaProcessorUtils.KERBEROS_CREDENTIALS_SERVICE, kerberosCredentialsService.getIdentifier());
        runner.assertNotValid();

        runner.removeProperty(KafkaProcessorUtils.SELF_CONTAINED_KERBEROS_USER_SERVICE);
        runner.assertValid();
    }

    private SelfContainedKerberosUserService enableKerberosUserService(final TestRunner runner) throws InitializationException {
        final SelfContainedKerberosUserService kerberosUserService = mock(SelfContainedKerberosUserService.class);
        when(kerberosUserService.getIdentifier()).thenReturn("userService1");
        runner.addControllerService(kerberosUserService.getIdentifier(), kerberosUserService);
        runner.enableControllerService(kerberosUserService);
        return kerberosUserService;
    }

    private KerberosCredentialsService enabledKerberosCredentialsService(final TestRunner runner) throws InitializationException {
        final KerberosCredentialsService credentialsService = mock(KerberosCredentialsService.class);
        when(credentialsService.getIdentifier()).thenReturn("credsService1");
        when(credentialsService.getPrincipal()).thenReturn("principal1");
        when(credentialsService.getKeytab()).thenReturn("keytab1");

        runner.addControllerService(credentialsService.getIdentifier(), credentialsService);
        runner.enableControllerService(credentialsService);
        return credentialsService;
    }

}
