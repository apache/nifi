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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfluentSchemaRegistryTest {

    private static final String SERVICE_ID = ConfluentSchemaRegistry.class.getSimpleName();

    private TestRunner runner;

    private ConfluentSchemaRegistry registry;

    @BeforeEach
    public void setUp() throws InitializationException {
        registry = new ConfluentSchemaRegistry();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
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

    @Test
    void testMigration() {
        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        final ConfluentSchemaRegistry confluentSchemaRegistry = new ConfluentSchemaRegistry();
        confluentSchemaRegistry.migrateProperties(configuration);

        Map<String, String> expected = Map.ofEntries(
                Map.entry("url", ConfluentSchemaRegistry.SCHEMA_REGISTRY_URLS.getName()),
                Map.entry("ssl-context", ConfluentSchemaRegistry.SSL_CONTEXT.getName()),
                Map.entry("cache-size", ConfluentSchemaRegistry.CACHE_SIZE.getName()),
                Map.entry("cache-expiration", ConfluentSchemaRegistry.CACHE_EXPIRATION.getName()),
                Map.entry("timeout", ConfluentSchemaRegistry.TIMEOUT.getName()),
                Map.entry("authentication-type", ConfluentSchemaRegistry.AUTHENTICATION_TYPE.getName()),
                Map.entry("username", ConfluentSchemaRegistry.USERNAME.getName()),
                Map.entry("password", ConfluentSchemaRegistry.PASSWORD.getName())
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }
}
