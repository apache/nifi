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

package org.apache.nifi.minifi.toolkit.schema;

import org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys;
import org.apache.nifi.minifi.toolkit.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.toolkit.schema.serialization.SchemaLoader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.ID_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ConfigSchemaTest {
    @Test
    public void testValid() throws IOException, SchemaLoaderException {
        final Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal-v2.yml"));
        final ConfigSchema configSchema = new ConfigSchema(yamlAsMap);
        final List<String> validationIssues = configSchema.getValidationIssues();
        assertEquals(0, validationIssues.size());
    }

    @Test
    public void testValidationIssuesFromOlder() throws IOException, SchemaLoaderException {
        final Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal.yml"));
        final ConfigSchema configSchema = new ConfigSchema(yamlAsMap);
        final List<String> validationIssues = configSchema.getValidationIssues();
        assertNotEquals(0, validationIssues.size());
    }

    @Test
    public void testProcessorDuplicateValidationNegativeCase() {
        final ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.PROCESSORS_KEY, getListWithKeyValues(ID_KEY, "testId1", "testId2")));
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS);
    }

    @Test
    public void testProcessorDuplicateValidationPositiveCase() {
        final ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.PROCESSORS_KEY, getListWithKeyValues(ID_KEY, "testId1", "testId1")));
        assertMessageDoesExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS);
    }

    @Test
    public void testConnectionDuplicateValidationNegativeCase() {
        final ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.CONNECTIONS_KEY, getListWithKeyValues(ID_KEY, "testId1", "testId2")));
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS);
    }

    @Test
    public void testConnectionDuplicateValidationPositiveCase() {
        final ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.CONNECTIONS_KEY, getListWithKeyValues(ID_KEY, "testId1", "testId1")));
        assertMessageDoesExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS);
    }

    @Test
    public void testInvalidSourceAndDestinationIds() throws IOException, SchemaLoaderException {
        final Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal-v2.yml"));
        final List<Map<String, Object>> connections = (List<Map<String, Object>>) yamlAsMap.get(CommonPropertyKeys.CONNECTIONS_KEY);
        assertEquals(1, connections.size());

        final String fakeSource = UUID.nameUUIDFromBytes("fakeSource".getBytes(StandardCharsets.UTF_8)).toString();
        final String fakeDestination = UUID.nameUUIDFromBytes("fakeDestination".getBytes(StandardCharsets.UTF_8)).toString();

        final Map<String, Object> connection = connections.get(0);
        connection.put(ConnectionSchema.SOURCE_ID_KEY, fakeSource);
        connection.put(ConnectionSchema.DESTINATION_ID_KEY, fakeDestination);

        final ConfigSchema configSchema = new ConfigSchema(yamlAsMap);
        final List<String> validationIssues = configSchema.getValidationIssues();
        assertEquals(new ArrayList<>(
                Arrays.asList(ConfigSchema.CONNECTION_WITH_ID + connection.get(ID_KEY) + ConfigSchema.HAS_INVALID_DESTINATION_ID + fakeDestination,
                        ConfigSchema.CONNECTION_WITH_ID + connection.get(ID_KEY) + ConfigSchema.HAS_INVALID_SOURCE_ID + fakeSource)), validationIssues);
    }

    @Test
    public void testNullNifiPropertyOverrides() {
        final ConfigSchema configSchema = new ConfigSchema(new HashMap<>());
        assertEquals(Collections.emptyMap(), configSchema.getNifiPropertiesOverrides());
        assertEquals(Collections.emptyMap(), configSchema.toMap().get(CommonPropertyKeys.NIFI_PROPERTIES_OVERRIDES_KEY));
    }

    @Test
    public void testEmptyNifiPropertyOverrides() {
        final Map<Object, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.NIFI_PROPERTIES_OVERRIDES_KEY, new HashMap<>());
        final ConfigSchema configSchema = new ConfigSchema(map);
        assertEquals(Collections.emptyMap(), configSchema.getNifiPropertiesOverrides());
        assertEquals(Collections.emptyMap(), configSchema.toMap().get(CommonPropertyKeys.NIFI_PROPERTIES_OVERRIDES_KEY));
    }

    @Test
    public void testNifiPropertyOverrides() {
        final Map<Object, Object> map = new HashMap<>();
        final Map<Object, Object> overrides = new HashMap<>();
        overrides.put("nifi.flowfile.repository.directory", "./flowfile_repository_override");
        overrides.put("nifi.content.repository.directory.default", "./content_repository_override");
        overrides.put("nifi.database.directory", "./database_repository_override");
        map.put(CommonPropertyKeys.NIFI_PROPERTIES_OVERRIDES_KEY, new HashMap<>(overrides));
        final ConfigSchema configSchema = new ConfigSchema(map);
        assertEquals(overrides, configSchema.getNifiPropertiesOverrides());
        assertEquals(overrides, configSchema.toMap().get(CommonPropertyKeys.NIFI_PROPERTIES_OVERRIDES_KEY));
    }

    public static List<Map<String, Object>> getListWithKeyValues(final String key, final String... values) {
        final List<Map<String, Object>> result = new ArrayList<>(values.length);
        for (final String value : values) {
            result.add(Collections.singletonMap(key, value));
        }
        return result;
    }

    public static void assertMessageDoesNotExist(final ConfigSchema configSchema, final String message) {
        for (final String validationIssue : configSchema.getValidationIssues()) {
            assertFalse(validationIssue.startsWith(message), "Did not expect to find message: " + validationIssue);
        }
    }

    public static void assertMessageDoesExist(final ConfigSchema configSchema, final String message) {
        for (final String validationIssue : configSchema.getValidationIssues()) {
            if (validationIssue.startsWith(message)) {
                return;
            }
        }
        fail("Expected to find message starting with: " + message);
    }
}
