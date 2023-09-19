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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNeo4JCypherClientService {

    private GraphClientService clientService;

    @BeforeEach
    public void setUp() {
        clientService = new Neo4JCypherClientService();
    }

    @Test
    public void testBuildQueryFromNodes() {
        final List<Map<String, Object>> nodeList = new ArrayList<>(3);
        nodeList.add(Collections.singletonMap("name", "Matt"));
        final Map<String,Object> node2 = new HashMap<>();
        node2.put("name", "Joe");
        node2.put("age", 40);
        node2.put("color", "blue");
        nodeList.add(node2);
        final Map<String,Object> node3 = new HashMap<>();
        node3.put("name", "Mary");
        node3.put("age", 40);
        node3.put("state", "FL");
        nodeList.add(node3);

        final List<GraphQuery> expectedQueryList = Arrays.asList(
                new GraphQuery("MERGE (p:NiFiProvenanceEvent {name: \"Matt\"})", GraphClientService.CYPHER),
                new GraphQuery("MERGE (p:NiFiProvenanceEvent {color: \"blue\",name: \"Joe\",age: \"40\"})", GraphClientService.CYPHER),
                new GraphQuery("MERGE (p:NiFiProvenanceEvent {name: \"Mary\",state: \"FL\",age: \"40\"})", GraphClientService.CYPHER)
        );
        final List<GraphQuery> queryList = clientService.buildQueryFromNodes(nodeList, new HashMap<>());
        assertEquals(expectedQueryList, queryList);
    }
}