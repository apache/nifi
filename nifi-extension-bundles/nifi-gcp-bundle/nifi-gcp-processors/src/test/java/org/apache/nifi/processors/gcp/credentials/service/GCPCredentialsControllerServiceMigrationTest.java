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
package org.apache.nifi.processors.gcp.credentials.service;

import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processors.gcp.credentials.factory.AuthenticationStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.LEGACY_USE_APPLICATION_DEFAULT_CREDENTIALS;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.LEGACY_USE_COMPUTE_ENGINE_CREDENTIALS;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON;
import static org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GCPCredentialsControllerServiceMigrationTest {

    private static final String SERVICE_ACCOUNT_JSON_PLACEHOLDER = "{\"mock\":\"json\"}";
    private final GCPCredentialsControllerService service = new GCPCredentialsControllerService();

    @Test
    void testMigratesApplicationDefaultFlag() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.LEGACY_USE_APPLICATION_DEFAULT_CREDENTIALS.getName(), "true");

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.APPLICATION_DEFAULT.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
        assertFalse(configuration.getRawProperties().containsKey(CredentialPropertyDescriptors.LEGACY_USE_APPLICATION_DEFAULT_CREDENTIALS.getName()));
    }

    @Test
    void testMigratesServiceAccountFile() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE.getName(), "/tmp/account.json");

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testMigratesServiceAccountJson() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON.getName(), SERVICE_ACCOUNT_JSON_PLACEHOLDER);

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testMigratesComputeEngineFlag() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.LEGACY_USE_COMPUTE_ENGINE_CREDENTIALS.getName(), "true");

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.COMPUTE_ENGINE.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
        assertFalse(configuration.getRawProperties().containsKey(CredentialPropertyDescriptors.LEGACY_USE_COMPUTE_ENGINE_CREDENTIALS.getName()));
    }

    @Test
    void testDoesNotOverrideExistingAuthenticationStrategy() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName(), AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue());
        properties.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE.getName(), "/tmp/account.json");

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.SERVICE_ACCOUNT_JSON.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testSetsDefaultAuthenticationStrategyWhenUnset() {
        final Map<String, String> properties = new HashMap<>();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);

        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.APPLICATION_DEFAULT.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testLegacyPropertiesDeriveAuthenticationStrategyWhenMissing() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE.getName(), "/tmp/account.json");

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testDoesNotOverrideExplicitStrategyWhenServiceAccountPropertiesSet() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName(), AuthenticationStrategy.APPLICATION_DEFAULT.getValue());
        properties.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE.getName(), "/tmp/account.json");

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(properties);
        service.migrateProperties(configuration);

        assertEquals(AuthenticationStrategy.APPLICATION_DEFAULT.getValue(),
                configuration.getRawProperties().get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY.getName()));
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("application-default-credentials", LEGACY_USE_APPLICATION_DEFAULT_CREDENTIALS.getName()),
                Map.entry("compute-engine-credentials", LEGACY_USE_COMPUTE_ENGINE_CREDENTIALS.getName()),
                Map.entry("service-account-json-file", SERVICE_ACCOUNT_JSON_FILE.getName()),
                Map.entry("service-account-json", SERVICE_ACCOUNT_JSON.getName()),
                Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE)
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}
