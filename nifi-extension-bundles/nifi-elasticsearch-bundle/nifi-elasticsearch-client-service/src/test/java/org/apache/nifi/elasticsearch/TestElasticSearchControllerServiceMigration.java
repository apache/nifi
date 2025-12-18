/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.elasticsearch;

import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestElasticSearchControllerServiceMigration {
    @Test
    void testElasticSearchClientServiceImplMigration() {
        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        ElasticSearchClientServiceImpl elasticSearchClientService = new ElasticSearchClientServiceImpl();
        elasticSearchClientService.migrateProperties(configuration);

        Map<String, String> expected = Map.ofEntries(
                Map.entry("el-cs-http-hosts", ElasticSearchClientService.HTTP_HOSTS.getName()),
                Map.entry("el-cs-ssl-context-service", ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE.getName()),
                Map.entry("authorization-scheme", ElasticSearchClientService.AUTHORIZATION_SCHEME.getName()),
                Map.entry("el-cs-oauth2-token-provider", ElasticSearchClientService.OAUTH2_ACCESS_TOKEN_PROVIDER.getName()),
                Map.entry("jwt-shared-secret", ElasticSearchClientService.JWT_SHARED_SECRET.getName()),
                Map.entry("el-cs-run-as-user", ElasticSearchClientService.RUN_AS_USER.getName()),
                Map.entry("el-cs-username", ElasticSearchClientService.USERNAME.getName()),
                Map.entry("el-cs-password", ElasticSearchClientService.PASSWORD.getName()),
                Map.entry("api-key-id", ElasticSearchClientService.API_KEY_ID.getName()),
                Map.entry("api-key", ElasticSearchClientService.API_KEY.getName()),
                Map.entry("el-cs-connect-timeout", ElasticSearchClientService.CONNECT_TIMEOUT.getName()),
                Map.entry("Connect timeout", ElasticSearchClientService.CONNECT_TIMEOUT.getName()),
                Map.entry("el-cs-socket-timeout", ElasticSearchClientService.SOCKET_TIMEOUT.getName()),
                Map.entry("el-cs-charset", ElasticSearchClientService.CHARSET.getName()),
                Map.entry("el-cs-suppress-nulls", ElasticSearchClientService.SUPPRESS_NULLS.getName()),
                Map.entry("el-cs-enable-compression", ElasticSearchClientService.COMPRESSION.getName()),
                Map.entry("el-cs-send-meta-header", ElasticSearchClientService.SEND_META_HEADER.getName()),
                Map.entry("el-cs-strict-deprecation", ElasticSearchClientService.STRICT_DEPRECATION.getName()),
                Map.entry("el-cs-node-selector", ElasticSearchClientService.NODE_SELECTOR.getName()),
                Map.entry("el-cs-path-prefix", ElasticSearchClientService.PATH_PREFIX.getName()),
                Map.entry("el-cs-sniff-cluster-nodes", ElasticSearchClientService.SNIFF_CLUSTER_NODES.getName()),
                Map.entry("el-cs-sniff-failure", ElasticSearchClientService.SNIFF_ON_FAILURE.getName()),
                Map.entry("el-cs-sniffer-interval", ElasticSearchClientService.SNIFFER_INTERVAL.getName()),
                Map.entry("el-cs-sniffer-request-timeout", ElasticSearchClientService.SNIFFER_REQUEST_TIMEOUT.getName()),
                Map.entry("el-cs-sniffer-failure-delay", ElasticSearchClientService.SNIFFER_FAILURE_DELAY.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ElasticSearchClientService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }

    @Test
    void testElasticSearchLookupServiceMigration() {
        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        ElasticSearchLookupService elasticSearchLookupService = new ElasticSearchLookupService();
        elasticSearchLookupService.migrateProperties(configuration);

        Map<String, String> expected = Map.ofEntries(
                Map.entry("el-rest-client-service", ElasticSearchLookupService.CLIENT_SERVICE.getName()),
                Map.entry("el-lookup-index", ElasticSearchLookupService.INDEX.getName()),
                Map.entry("el-lookup-type", ElasticSearchLookupService.TYPE.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_ACCESS_STRATEGY_PROPERTY_NAME, SCHEMA_ACCESS_STRATEGY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REGISTRY_PROPERTY_NAME, SCHEMA_REGISTRY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_NAME_PROPERTY_NAME, SCHEMA_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_BRANCH_NAME_PROPERTY_NAME, SCHEMA_BRANCH_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_VERSION_PROPERTY_NAME, SCHEMA_VERSION.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_TEXT_PROPERTY_NAME, SCHEMA_TEXT.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REFERENCE_READER_PROPERTY_NAME, SCHEMA_REFERENCE_READER.getName())
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }

    @Test
    void testElasticSearchStringLookupServiceMigration() {
        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        ElasticSearchStringLookupService elasticSearchLookupService = new ElasticSearchStringLookupService();
        elasticSearchLookupService.migrateProperties(configuration);

        Map<String, String> expected = Map.ofEntries(
                Map.entry("el-rest-client-service", ElasticSearchStringLookupService.CLIENT_SERVICE.getName()),
                Map.entry("el-lookup-index", ElasticSearchStringLookupService.INDEX.getName()),
                Map.entry("el-lookup-type", ElasticSearchStringLookupService.TYPE.getName())
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }
}
