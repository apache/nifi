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

package org.apache.nifi.yaml;

import org.apache.nifi.json.AbstractJsonRowRecordReader;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.OBSOLETE_SCHEMA_CACHE;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.SCHEMA_CACHE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestYamlTreeReader {
    @Test
    void testGetSupportedPropertyDescriptors() {
        final YamlTreeReader service = new YamlTreeReader();
        assertFalse(service.getSupportedPropertyDescriptors().contains(AbstractJsonRowRecordReader.PARSING_STRATEGY));
    }

    @ParameterizedTest
    @MethodSource("migrationConfigurations")
    void testMigrateProperties(MockPropertyConfiguration configuration, Set<String> expectedRemoved) {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("starting-field-strategy", JsonTreeReader.STARTING_FIELD_STRATEGY.getName()),
                Map.entry("starting-field-name", JsonTreeReader.STARTING_FIELD_NAME.getName()),
                Map.entry("schema-application-strategy", JsonTreeReader.SCHEMA_APPLICATION_STRATEGY.getName()),
                Map.entry(OBSOLETE_SCHEMA_CACHE, SCHEMA_CACHE.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_ACCESS_STRATEGY_PROPERTY_NAME, SCHEMA_ACCESS_STRATEGY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REGISTRY_PROPERTY_NAME, SCHEMA_REGISTRY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_NAME_PROPERTY_NAME, SCHEMA_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_BRANCH_NAME_PROPERTY_NAME, SCHEMA_BRANCH_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_VERSION_PROPERTY_NAME, SCHEMA_VERSION.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_TEXT_PROPERTY_NAME, SCHEMA_TEXT.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REFERENCE_READER_PROPERTY_NAME, SCHEMA_REFERENCE_READER.getName())
        );

        final YamlTreeReader service = new YamlTreeReader();
        service.migrateProperties(configuration);
        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();

        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();
        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<String> propertiesRemoved = result.getPropertiesRemoved();
        assertEquals(expectedRemoved, propertiesRemoved);
    }

    private static Stream<Arguments> migrationConfigurations() {
        return Stream.of(
                Arguments.argumentSet("Configuration without allow comments",
                        new MockPropertyConfiguration(Map.of()), Set.of(AbstractJsonRowRecordReader.PARSING_STRATEGY.getName())),
                Arguments.argumentSet("Configuration with allow comments",
                        new MockPropertyConfiguration(Map.of(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS, "true")),
                        Set.of(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS, AbstractJsonRowRecordReader.PARSING_STRATEGY.getName()))
        );
    }
}
