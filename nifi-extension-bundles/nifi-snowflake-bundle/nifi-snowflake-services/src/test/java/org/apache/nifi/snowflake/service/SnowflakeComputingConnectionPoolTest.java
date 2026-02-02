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
package org.apache.nifi.snowflake.service;

import net.snowflake.client.core.SFSessionProperty;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnowflakeComputingConnectionPoolTest {

    private static final String PRIVATE_KEY_SERVICE_ID = PrivateKeyService.class.getSimpleName();

    private static final String RSA_ALGORITHM = "RSA";

    private static final String SNOWFLAKE_JWT = "SNOWFLAKE_JWT";

    private static final String PEM_HEADER = "-----BEGIN PRIVATE KEY-----";

    private static final String PEM_HEADER_BASE64 = Base64.getEncoder().encodeToString(PEM_HEADER.getBytes(StandardCharsets.UTF_8));

    private static final String PEM_CONTENT_FORMAT = "%s%n%s%n-----END PRIVATE KEY-----%n";

    private static PrivateKey privateKey;

    private static String pemPrivateKeyBase64;

    @Mock
    private PrivateKeyService privateKeyService;

    private SnowflakeComputingConnectionPool pool;

    @BeforeAll
    static void setPrivateKey() throws GeneralSecurityException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        privateKey = keyPair.getPrivate();
        final byte[] privateKeyEncoded = privateKey.getEncoded();
        final String privateKeyEncodedBase64 = Base64.getEncoder().encodeToString(privateKeyEncoded);
        final String pemPrivateKey = PEM_CONTENT_FORMAT.formatted(PEM_HEADER, privateKeyEncodedBase64);
        pemPrivateKeyBase64 = Base64.getEncoder().encodeToString(pemPrivateKey.getBytes(StandardCharsets.UTF_8));
    }

    @BeforeEach
    void setPool() {
        pool = new SnowflakeComputingConnectionPool();
    }

    @Test
    void testGetConnectionPropertiesPrivateKeyService() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SnowflakeComputingConnectionPool.PRIVATE_KEY_SERVICE, PRIVATE_KEY_SERVICE_ID);

        final MockProcessContext controllerServiceLookup = new MockProcessContext(new NoOpProcessor());
        when(privateKeyService.getIdentifier()).thenReturn(PRIVATE_KEY_SERVICE_ID);
        controllerServiceLookup.addControllerService(privateKeyService, Map.of(), null);

        final Map<String, String> environmentVariables = Map.of();
        final MockConfigurationContext context = new MockConfigurationContext(properties, controllerServiceLookup, environmentVariables);

        when(privateKeyService.getPrivateKey()).thenReturn(privateKey);
        final Map<String, String> connectionProperties = pool.getConnectionProperties(context);
        assertNotNull(connectionProperties);
        assertFalse(connectionProperties.isEmpty());

        final String authenticator = connectionProperties.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey());
        assertEquals(SNOWFLAKE_JWT, authenticator);

        final String privateKeyBase64 = connectionProperties.get(SFSessionProperty.PRIVATE_KEY_BASE64.getPropertyKey());
        assertNotNull(privateKeyBase64);

        assertTrue(privateKeyBase64.startsWith(PEM_HEADER_BASE64), "PEM Header encoded with Bas64 not found");
        assertEquals(pemPrivateKeyBase64, privateKeyBase64);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("connection-url-format", SnowflakeComputingConnectionPool.CONNECTION_URL_FORMAT.getName()),
                Map.entry("warehouse", SnowflakeComputingConnectionPool.SNOWFLAKE_WAREHOUSE.getName()),
                Map.entry(SnowflakeProperties.OLD_ACCOUNT_LOCATOR_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_LOCATOR.getName()),
                Map.entry(SnowflakeProperties.OLD_CLOUD_REGION_PROPERTY_NAME, SnowflakeProperties.CLOUD_REGION.getName()),
                Map.entry(SnowflakeProperties.OLD_CLOUD_TYPE_PROPERTY_NAME, SnowflakeProperties.CLOUD_TYPE.getName()),
                Map.entry(SnowflakeProperties.OLD_ORGANIZATION_NAME_PROPERTY_NAME, SnowflakeProperties.ORGANIZATION_NAME.getName()),
                Map.entry(SnowflakeProperties.OLD_ACCOUNT_NAME_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_NAME.getName()),
                Map.entry(SnowflakeProperties.OLD_DATABASE_PROPERTY_NAME, SnowflakeProperties.DATABASE.getName()),
                Map.entry(SnowflakeProperties.OLD_SCHEMA_PROPERTY_NAME, SnowflakeProperties.SCHEMA.getName()),
                Map.entry(DBCPProperties.OLD_VALIDATION_QUERY_PROPERTY_NAME, VALIDATION_QUERY.getName()),
                Map.entry(DBCPProperties.OLD_MIN_IDLE_PROPERTY_NAME, MIN_IDLE.getName()),
                Map.entry(DBCPProperties.OLD_MAX_IDLE_PROPERTY_NAME, MAX_IDLE.getName()),
                Map.entry(DBCPProperties.OLD_MAX_CONN_LIFETIME_PROPERTY_NAME, MAX_CONN_LIFETIME.getName()),
                Map.entry(DBCPProperties.OLD_EVICTION_RUN_PERIOD_PROPERTY_NAME, EVICTION_RUN_PERIOD.getName()),
                Map.entry(DBCPProperties.OLD_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, MIN_EVICTABLE_IDLE_TIME.getName()),
                Map.entry(DBCPProperties.OLD_SOFT_MIN_EVICTABLE_IDLE_TIME_PROPERTY_NAME, SOFT_MIN_EVICTABLE_IDLE_TIME.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        pool.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}
