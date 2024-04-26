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
package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelegatingLoginConfigProviderTest {

    private static final String PLAIN_LOGIN_MODULE = "PlainLoginModule";
    private static final String SCRAM_LOGIN_MODULE = "ScramLoginModule";

    DelegatingLoginConfigProvider provider;

    TestRunner runner;

    @BeforeEach
    void setProvider() {
        provider = new DelegatingLoginConfigProvider();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.setValidateExpressionUsage(false);
    }

    @Test
    void testGetConfigurationPlain() {
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.PLAIN);
        final PropertyContext propertyContext = runner.getProcessContext();

        final String configuration = provider.getConfiguration(propertyContext);

        assertNotNull(configuration);
        assertTrue(configuration.contains(PLAIN_LOGIN_MODULE), "PLAIN configuration not found");
    }

    @Test
    void testGetConfigurationScram() {
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.SCRAM_SHA_512);
        final PropertyContext propertyContext = runner.getProcessContext();

        final String configuration = provider.getConfiguration(propertyContext);

        assertNotNull(configuration);
        assertTrue(configuration.contains(SCRAM_LOGIN_MODULE), "SCRAM configuration not found");
    }
}
