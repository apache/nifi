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

import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStandardSnowflakeIngestManagerProviderService {
    @Test
    void testMigrateProperties() {
        final StandardSnowflakeIngestManagerProviderService service = new StandardSnowflakeIngestManagerProviderService();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("account-identifier-format", StandardSnowflakeIngestManagerProviderService.ACCOUNT_IDENTIFIER_FORMAT.getName()),
                Map.entry("host-url", StandardSnowflakeIngestManagerProviderService.HOST_URL.getName()),
                Map.entry("user-name", StandardSnowflakeIngestManagerProviderService.USER_NAME.getName()),
                Map.entry("private-key-service", StandardSnowflakeIngestManagerProviderService.PRIVATE_KEY_SERVICE.getName()),
                Map.entry("pipe", StandardSnowflakeIngestManagerProviderService.PIPE.getName()),
                Map.entry(SnowflakeProperties.OLD_ACCOUNT_LOCATOR_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_LOCATOR.getName()),
                Map.entry(SnowflakeProperties.OLD_CLOUD_REGION_PROPERTY_NAME, SnowflakeProperties.CLOUD_REGION.getName()),
                Map.entry(SnowflakeProperties.OLD_CLOUD_TYPE_PROPERTY_NAME, SnowflakeProperties.CLOUD_TYPE.getName()),
                Map.entry(SnowflakeProperties.OLD_ORGANIZATION_NAME_PROPERTY_NAME, SnowflakeProperties.ORGANIZATION_NAME.getName()),
                Map.entry(SnowflakeProperties.OLD_ACCOUNT_NAME_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_NAME.getName()),
                Map.entry(SnowflakeProperties.OLD_DATABASE_PROPERTY_NAME, SnowflakeProperties.DATABASE.getName()),
                Map.entry(SnowflakeProperties.OLD_SCHEMA_PROPERTY_NAME, SnowflakeProperties.SCHEMA.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}
