/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.commons.schema.v1;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConfigSchemaTest;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ConfigSchemaV1Test {
    @Test
    public void testValid() throws IOException, SchemaLoaderException {
        Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal.yml"));
        ConfigSchema configSchema = new ConfigSchemaV1(yamlAsMap).convert();
        List<String> validationIssues = configSchema.getValidationIssues();
        assertEquals(new ArrayList<>(), validationIssues);
    }
    @Test
    public void testValidationIssuesFromNewer() throws IOException, SchemaLoaderException {
        Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal-v2.yml"));
        ConfigSchema configSchema = new ConfigSchemaV1(yamlAsMap).convert();
        List<String> validationIssues = configSchema.getValidationIssues();
        assertNotEquals(0, validationIssues.size());
    }

    @Test
    public void testInvalidSourceAndDestinationNames() throws IOException, SchemaLoaderException {
        Map<String, Object> yamlAsMap = SchemaLoader.loadYamlAsMap(ConfigSchemaTest.class.getClassLoader().getResourceAsStream("config-minimal.yml"));
        List<Map<String, Object>> connections = (List<Map<String, Object>>) yamlAsMap.get(CommonPropertyKeys.CONNECTIONS_KEY);
        assertEquals(1, connections.size());

        String fakeSource = "fakeSource";
        String fakeDestination = "fakeDestination";

        Map<String, Object> connection = connections.get(0);
        connection.put(ConnectionSchemaV1.SOURCE_NAME_KEY, fakeSource);
        connection.put(ConnectionSchemaV1.DESTINATION_NAME_KEY, fakeDestination);

        ConfigSchema configSchema = new ConfigSchemaV1(yamlAsMap).convert();
        List<String> validationIssues = configSchema.getValidationIssues();
        assertEquals(4, validationIssues.size());
        assertEquals(BaseSchema.getIssueText(ConnectionSchema.DESTINATION_ID_KEY, "Connection(id: 0401b747-1dca-31c7-ab4b-cdacf7e6c44b, name: TailToSplit)",
                BaseSchema.IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED), validationIssues.get(0));
        assertEquals(BaseSchema.getIssueText(ConnectionSchema.SOURCE_ID_KEY, "Connection(id: 0401b747-1dca-31c7-ab4b-cdacf7e6c44b, name: TailToSplit)", BaseSchema.IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED),
                validationIssues.get(1));
        assertEquals(ConfigSchemaV1.CONNECTION_WITH_NAME + connection.get(NAME_KEY) + ConfigSchemaV1.HAS_INVALID_DESTINATION_NAME + fakeDestination, validationIssues.get(2));
        assertEquals(ConfigSchemaV1.CONNECTION_WITH_NAME + connection.get(NAME_KEY) + ConfigSchemaV1.HAS_INVALID_SOURCE_NAME + fakeSource, validationIssues.get(3));
    }

    @Test
    public void testGetUniqueIdConflicts() {
        Set<UUID> ids = new HashSet<>();
        assertEquals("56e7ae5d-aae1-351e-bca9-cdc4446c6386", ConfigSchemaV1.getUniqueId(ids, "test/id"));
        assertEquals("348c4f93-cd15-3d91-82f2-bfe5d43834d8", ConfigSchemaV1.getUniqueId(ids, "test$id"));
        assertEquals("348c4f93-cd15-3d91-82f2-bfe5d43834d9", ConfigSchemaV1.getUniqueId(ids, "test$id"));
        assertEquals("348c4f93-cd15-3d91-82f2-bfe5d43834da", ConfigSchemaV1.getUniqueId(ids, "test$id"));
        assertEquals("348c4f93-cd15-3d91-82f2-bfe5d43834db", ConfigSchemaV1.getUniqueId(ids, "test$id"));
        assertEquals("697487ff-979c-342d-9aab-5c710ae43a28", ConfigSchemaV1.getUniqueId(ids, "test_id_2"));
    }

    @Test
    public void testGetUniqueIdEmptySet() {
        String testId = "testId";
        assertEquals("17841b04-ce22-34a3-9603-d95ec31d02dc", ConfigSchemaV1.getUniqueId(new HashSet<>(), testId + "/ $"));
    }
}
