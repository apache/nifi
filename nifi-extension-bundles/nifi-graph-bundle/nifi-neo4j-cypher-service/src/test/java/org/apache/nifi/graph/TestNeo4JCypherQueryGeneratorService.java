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

import org.apache.nifi.processor.exception.ProcessException;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestNeo4JCypherQueryGeneratorService {

    @Test
    void testGenerateSetPropertiesMutationForNode() {
        final Neo4JCypherQueryGeneratorService service = new Neo4JCypherQueryGeneratorService();
        final LinkedHashMap<String, Object> identifiers = new LinkedHashMap<>();
        identifiers.put("id", 123L);
        final Map<String, Object> properties = Map.of("price", 100, "age", "10");

        final GraphMutation mutation = service.generateSetPropertiesMutation(GraphElementType.NODE, identifiers, "Person", properties);

        final String expectedQuery = "MERGE (n:Person {id: $identifier_0})\n"
                + "ON MATCH SET n += $properties\n"
                + "ON CREATE SET n += $properties";

        assertEquals(expectedQuery, mutation.getQuery());
        assertEquals(123L, mutation.getParameters().get("identifier_0"));
        assertEquals(properties, mutation.getParameters().get("properties"));
    }

    @Test
    void testGenerateSetPropertiesMutationForEdgeUsesMatchQuery() {
        final Neo4JCypherQueryGeneratorService service = new Neo4JCypherQueryGeneratorService();
        final LinkedHashMap<String, Object> identifiers = new LinkedHashMap<>();
        identifiers.put("edgeId", "123");
        final Map<String, Object> properties = Map.of("weight", 7);

        final GraphMutation mutation = service.generateSetPropertiesMutation(GraphElementType.EDGE, identifiers, "ASSOCIATED_WITH", properties);

        final String expectedQuery = "MATCH ()-[e:ASSOCIATED_WITH {edgeId: $identifier_0}]-()\n"
                + "SET e += $properties";

        assertEquals(expectedQuery, mutation.getQuery());
        assertEquals("123", mutation.getParameters().get("identifier_0"));
        assertEquals(properties, mutation.getParameters().get("properties"));
    }

    @Test
    void testGenerateSetPropertiesMutationForEdgeRequiresLabel() {
        final Neo4JCypherQueryGeneratorService service = new Neo4JCypherQueryGeneratorService();

        assertThrows(ProcessException.class, () -> service.generateSetPropertiesMutation(GraphElementType.EDGE, new LinkedHashMap<>(), null, Map.of()));
    }

    @Test
    void testGenerateSetPropertiesMutationRejectsNullIdentifierValue() {
        final Neo4JCypherQueryGeneratorService service = new Neo4JCypherQueryGeneratorService();
        final LinkedHashMap<String, Object> identifiers = new LinkedHashMap<>();
        identifiers.put("id", null);

        assertThrows(ProcessException.class, () -> service.generateSetPropertiesMutation(GraphElementType.NODE, identifiers, "Person", Map.of()));
    }

    @Test
    void testGenerateSetPropertiesMutationRejectsInvalidToken() {
        final Neo4JCypherQueryGeneratorService service = new Neo4JCypherQueryGeneratorService();
        final LinkedHashMap<String, Object> identifiers = new LinkedHashMap<>();
        identifiers.put("edge-id", "123");

        assertThrows(ProcessException.class, () -> service.generateSetPropertiesMutation(GraphElementType.EDGE, identifiers, "ASSOCIATED_WITH", Map.of()));
    }
}
