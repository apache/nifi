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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TestConsumeKafka_2_0 {

    ConsumerLease mockLease = null;
    ConsumerPool mockConsumerPool = null;

    @Before
    public void setup() {
        mockLease = mock(ConsumerLease.class);
        mockConsumerPool = mock(ConsumerPool.class);
    }

    @Test
    public void validateCustomValidatorSettings() throws Exception {
        ConsumeKafka_2_0 consumeKafka = new ConsumeKafka_2_0();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_2_0.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_2_0.AUTO_OFFSET_RESET, ConsumeKafka_2_0.OFFSET_EARLIEST);
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
        ConsumeKafka_2_0 consumeKafka = new ConsumeKafka_2_0();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_2_0.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_2_0.AUTO_OFFSET_RESET, ConsumeKafka_2_0.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafka_2_0.GROUP_ID);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("invalid because Group ID is required"));
        }

        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, "  ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
    }

    @Test
    public void testJaasConfiguration() throws Exception {
        ConsumeKafka_2_0 consumeKafka = new ConsumeKafka_2_0();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka_2_0.TOPICS, "foo");
        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka_2_0.AUTO_OFFSET_RESET, ConsumeKafka_2_0.OFFSET_EARLIEST);

        runner.setProperty(KafkaProcessorUtils.SECURITY_PROTOCOL, KafkaProcessorUtils.SEC_SASL_PLAINTEXT);
        runner.assertNotValid();

        runner.setProperty(KafkaProcessorUtils.JAAS_SERVICE_NAME, "kafka");
        runner.assertValid();

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
        runner.setProperty(KafkaProcessorUtils.USER_KEYTAB, "${keytab}s");
        runner.setProperty(KafkaProcessorUtils.JAAS_SERVICE_NAME, "${service}");
        runner.assertValid();
    }

}
