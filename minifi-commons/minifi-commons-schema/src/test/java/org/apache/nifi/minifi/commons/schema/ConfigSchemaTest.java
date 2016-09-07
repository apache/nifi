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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ConfigSchemaTest {
    @Test
    public void testGetUniqueIdEmptySet() {
        String testId = "testId";
        assertEquals(testId + "___", ConfigSchema.getUniqueId(new HashMap<>(), testId + "/ $"));
    }

    @Test
    public void testConnectionGeneratedIds() {
        List<Map<String, Object>> listWithKeyValues = getListWithKeyValues(CommonPropertyKeys.NAME_KEY, "test", "test", "test_2");

        // These ids should be honored even though they're last
        listWithKeyValues.addAll(getListWithKeyValues(CommonPropertyKeys.ID_KEY, "test", "test_2"));

        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.CONNECTIONS_KEY, listWithKeyValues));
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS);
        List<ConnectionSchema> connections = configSchema.getConnections();
        assertEquals(5, connections.size());

        // Generated unique ids
        assertEquals("test_3", connections.get(0).getId());
        assertEquals("test_4", connections.get(1).getId());
        assertEquals("test_2_2", connections.get(2).getId());

        // Specified ids
        assertEquals("test", connections.get(3).getId());
        assertEquals("test_2", connections.get(4).getId());
    }

    @Test
    public void testGetUniqueIdConflicts() {
        Map<String, Integer> ids = new HashMap<>();
        assertEquals("test_id", ConfigSchema.getUniqueId(ids, "test/id"));
        assertEquals("test_id_2", ConfigSchema.getUniqueId(ids, "test$id"));
        assertEquals("test_id_3", ConfigSchema.getUniqueId(ids, "test$id"));
        assertEquals("test_id_4", ConfigSchema.getUniqueId(ids, "test$id"));
        assertEquals("test_id_5", ConfigSchema.getUniqueId(ids, "test$id"));
        assertEquals("test_id_2_2", ConfigSchema.getUniqueId(ids, "test_id_2"));
    }

    @Test
    public void testProcessorDuplicateValidationNegativeCase() {
        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.PROCESSORS_KEY, getListWithKeyValues(CommonPropertyKeys.ID_KEY, "testId1", "testId2")));
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS);
    }

    @Test
    public void testProcessorDuplicateValidationPositiveCase() {
        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.PROCESSORS_KEY, getListWithKeyValues(CommonPropertyKeys.ID_KEY, "testId1", "testId1")));
        assertMessageDoesExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_PROCESSOR_IDS);
    }

    @Test
    public void testConnectionDuplicateValidationNegativeCase() {
        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.CONNECTIONS_KEY, getListWithKeyValues(CommonPropertyKeys.ID_KEY, "testId1", "testId2")));
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS);
    }

    @Test
    public void testConnectionDuplicateValidationPositiveCase() {
        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.CONNECTIONS_KEY, getListWithKeyValues(CommonPropertyKeys.ID_KEY, "testId1", "testId1")));
        assertMessageDoesExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS);
    }

    @Test
    public void testRemoteProcessingGroupDuplicateValidationNegativeCase() {
        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.REMOTE_PROCESSING_GROUPS_KEY, getListWithNames("testName1", "testName2")));
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES);
    }

    @Test
    public void testRemoteProcessingGroupDuplicateValidationPositiveCase() {
        ConfigSchema configSchema = new ConfigSchema(Collections.singletonMap(CommonPropertyKeys.REMOTE_PROCESSING_GROUPS_KEY, getListWithNames("testName1", "testName1")));
        assertMessageDoesExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_REMOTE_PROCESSING_GROUP_NAMES);
    }

    @Test
    public void testInvalidSourceAndDestinationNames() throws IOException, SchemaLoaderException {
        Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal.yml"));
        List<Map<String, Object>> connections = (List<Map<String, Object>>) yamlAsMap.get(CommonPropertyKeys.CONNECTIONS_KEY);
        assertEquals(1, connections.size());

        String fakeSource = "fakeSource";
        String fakeDestination = "fakeDestination";

        Map<String, Object> connection = connections.get(0);
        connection.put(ConnectionSchema.SOURCE_NAME_KEY, fakeSource);
        connection.put(ConnectionSchema.DESTINATION_NAME_KEY, fakeDestination);

        ConfigSchema configSchema = new ConfigSchema(yamlAsMap);
        List<String> validationIssues = configSchema.getValidationIssues();
        assertEquals(3, validationIssues.size());
        assertEquals(ConfigSchema.CONNECTIONS_REFER_TO_PROCESSOR_NAMES_THAT_DONT_EXIST + fakeDestination + ", " + fakeSource, validationIssues.get(0));
        assertEquals(BaseSchema.getIssueText(ConnectionSchema.SOURCE_ID_KEY, CommonPropertyKeys.CONNECTIONS_KEY, BaseSchema.IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED), validationIssues.get(1));
        assertEquals(BaseSchema.getIssueText(ConnectionSchema.DESTINATION_ID_KEY, CommonPropertyKeys.CONNECTIONS_KEY, BaseSchema.IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED), validationIssues.get(2));
    }

    public static List<Map<String, Object>> getListWithNames(String... names) {
        return getListWithKeyValues(CommonPropertyKeys.NAME_KEY, names);
    }

    public static List<Map<String, Object>> getListWithKeyValues(String key, String... values) {
        List<Map<String, Object>> result = new ArrayList<>(values.length);
        for (String value : values) {
            result.add(Collections.singletonMap(key, value));
        }
        return result;
    }

    public static void assertMessageDoesNotExist(ConfigSchema configSchema, String message) {
        for (String validationIssue : configSchema.getValidationIssues()) {
            assertFalse("Did not expect to find message: " + validationIssue, validationIssue.startsWith(message));
        }
    }

    public static void assertMessageDoesExist(ConfigSchema configSchema, String message) {
        for (String validationIssue : configSchema.getValidationIssues()) {
            if (validationIssue.startsWith(message)) {
                return;
            }
        }
        fail("Expected to find message starting with: " + message);
    }
}
