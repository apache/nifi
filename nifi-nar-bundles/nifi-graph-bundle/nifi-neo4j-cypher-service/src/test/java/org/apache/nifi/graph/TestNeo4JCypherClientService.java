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
        final Map<String,Object> node1 = new HashMap<>();
        node1.put("entityId", "Joe");
        node1.put("eventOrdinal", 40);
        node1.put("entityType", "male");
        nodeList.add(node1);
        final Map<String,Object> node2 = new HashMap<>();
        node2.put("entityId", "Mary");
        node2.put("eventOrdinal", 40);
        node2.put("entityType", "female");
        nodeList.add(node2);

        final List<GraphQuery> expectedQueryList = Arrays.asList(
                new GraphQuery("MERGE (p:`null`: NiFiProvenanceEvent {id: 'null'})\n" +
                        "ON CREATE SET \n" +
                        "\tp.eventOrdinal =40,\n" +
                        "\tp.entityType ='male',\n" +
                        "\tp.entityId ='Joe'\n" +
                        "ON MATCH SET \n" +
                        "\tp.eventOrdinal =40,\n" +
                        "\tp.entityType ='male',\n" +
                        "\tp.entityId ='Joe'", GraphClientService.CYPHER),
                new GraphQuery("MERGE (ff:`male` {id: 'Joe', type:'male'})", GraphClientService.CYPHER),
                new GraphQuery("MATCH (x:`male` {id: 'Joe'}),\n" +
                        "(y:NiFiProvenanceEvent {eventOrdinal: '40'})\n" +
                        "MERGE(x) <-[:entity]- (y)", GraphClientService.CYPHER),
                new GraphQuery("MERGE (p:`null`: NiFiProvenanceEvent {id: 'null'})\n" +
                        "ON CREATE SET \n" +
                        "\tp.eventOrdinal =40,\n" +
                        "\tp.entityType ='female',\n" +
                        "\tp.entityId ='Mary'\n" +
                        "ON MATCH SET \n" +
                        "\tp.eventOrdinal =40,\n" +
                        "\tp.entityType ='female',\n" +
                        "\tp.entityId ='Mary'", GraphClientService.CYPHER),
                new GraphQuery("MERGE (ff:`female` {id: 'Mary', type:'female'})", GraphClientService.CYPHER),
                new GraphQuery("MATCH (x:`female` {id: 'Mary'}),\n" +
                        "(y:NiFiProvenanceEvent {eventOrdinal: '40'})\n" +
                        "MERGE(x) <-[:entity]- (y)", GraphClientService.CYPHER)
        );
        final List<GraphQuery> queryList = clientService.buildProvenanceQueriesFromNodes(nodeList, new HashMap<>(), false);
        assertEquals(expectedQueryList, queryList);
    }
}