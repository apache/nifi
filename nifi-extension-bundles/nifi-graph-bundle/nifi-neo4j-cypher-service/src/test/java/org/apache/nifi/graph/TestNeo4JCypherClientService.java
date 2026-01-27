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
package org.apache.nifi.graph;

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNeo4JCypherClientService {
    @Test
    void testMigrateProperties() {
        final Neo4JCypherClientService service = new Neo4JCypherClientService();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("neo4j-connection-url", Neo4JCypherClientService.CONNECTION_URL.getName()),
                Map.entry("neo4j-username", Neo4JCypherClientService.USERNAME.getName()),
                Map.entry("neo4j-password", Neo4JCypherClientService.PASSWORD.getName()),
                Map.entry("neo4j-max-connection-time-out", Neo4JCypherClientService.CONNECTION_TIMEOUT.getName()),
                Map.entry("neo4j-max-connection-pool-size", Neo4JCypherClientService.MAX_CONNECTION_POOL_SIZE.getName()),
                Map.entry("neo4j-max-connection-acquisition-timeout", Neo4JCypherClientService.MAX_CONNECTION_ACQUISITION_TIMEOUT.getName()),
                Map.entry("neo4j-idle-time-before-test", Neo4JCypherClientService.IDLE_TIME_BEFORE_CONNECTION_TEST.getName()),
                Map.entry("neo4j-max-connection-lifetime", Neo4JCypherClientService.MAX_CONNECTION_LIFETIME.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}
