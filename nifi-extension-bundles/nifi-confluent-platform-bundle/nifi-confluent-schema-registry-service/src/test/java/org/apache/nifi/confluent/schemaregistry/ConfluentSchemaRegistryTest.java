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
package org.apache.nifi.confluent.schemaregistry;

import org.apache.nifi.confluent.schemaregistry.client.AuthenticationType;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConfluentSchemaRegistryTest {

    private static final String SERVICE_ID = ConfluentSchemaRegistry.class.getSimpleName();

    private TestRunner runner;

    private ConfluentSchemaRegistry registry;

    @BeforeEach
    public void setUp() throws InitializationException {
        registry = new ConfluentSchemaRegistry();
        final Processor processor = Mockito.mock(Processor.class);
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(SERVICE_ID, registry);
    }

    @Test
    public void testValidateAuthenticationTypeBasicMissingUsernameAndPassword() {
        runner.setProperty(registry, ConfluentSchemaRegistry.AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString());
        runner.assertNotValid(registry);
    }

    @Test
    public void testValidateAuthenticationTypeBasicMissingUsername() {
        runner.setProperty(registry, ConfluentSchemaRegistry.AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString());
        runner.setProperty(registry, ConfluentSchemaRegistry.PASSWORD, String.class.getName());
        runner.assertNotValid(registry);
    }

    @Test
    public void testValidateAuthenticationTypeBasicMissingPassword() {
        runner.setProperty(registry, ConfluentSchemaRegistry.AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString());
        runner.setProperty(registry, ConfluentSchemaRegistry.USERNAME, String.class.getSimpleName());
        runner.assertNotValid(registry);
    }

    @Test
    public void testValidateAuthenticationTypeNoneValid() {
        runner.setProperty(registry, ConfluentSchemaRegistry.AUTHENTICATION_TYPE, AuthenticationType.NONE.toString());
        runner.assertValid(registry);
    }

    @Test
    public void testValidateAndEnableDefaultProperties() {
        runner.assertValid(registry);
        runner.enableControllerService(registry);
    }

    @Test
    public void testValidateAndEnableAuthenticationTypeBasic() {
        runner.setProperty(registry, ConfluentSchemaRegistry.AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString());
        runner.setProperty(registry, ConfluentSchemaRegistry.USERNAME, String.class.getSimpleName());
        runner.setProperty(registry, ConfluentSchemaRegistry.PASSWORD, String.class.getName());
        runner.assertValid(registry);
        runner.enableControllerService(registry);
    }

    @Test
    public void testValidateAndEnableDynamicHttpHeaderProperties() {
        runner.setProperty(registry, "request.header.User", "kafkaUser");
        runner.setProperty(registry, "request.header.Test", "testValue");
        runner.assertValid(registry);
        runner.enableControllerService(registry);
    }

    @Test
    public void testValidateDynamicHttpHeaderPropertiesMissingTrailingValue() {
        runner.setProperty(registry, "request.header.", "NotValid");
        runner.assertNotValid(registry);
    }

    @Test
    public void testValidateDynamicHttpHeaderPropertiesInvalidSubject() {
        runner.setProperty(registry, "not.valid.subject", "NotValid");
        runner.assertNotValid(registry);
    }
}
