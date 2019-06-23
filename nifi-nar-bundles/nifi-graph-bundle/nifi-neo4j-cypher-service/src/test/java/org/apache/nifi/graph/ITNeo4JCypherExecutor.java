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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Neo4J Cypher integration tests.  Please set the neo4j url, user and password according to your setup.
 */
public class ITNeo4JCypherExecutor {
    protected TestRunner runner;
    protected Driver driver;
    protected String neo4jUrl = "bolt://localhost:7687";
    protected String user = "neo4j";
    protected String password = "testing1234";

    private GraphClientService clientService;
    private GraphQueryResultCallback EMPTY_CALLBACK = (record, hasMore) -> {};

    @Before
    public void setUp() throws Exception {
        clientService = new Neo4JCypherClientService();
        runner = TestRunners.newTestRunner(MockProcessor.class);
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, Neo4JCypherClientService.USERNAME, user);
        runner.setProperty(clientService, Neo4JCypherClientService.PASSWORD, password);
        runner.enableControllerService(clientService);
        runner.setProperty(MockProcessor.CLIENT, "clientService");
        driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic(user, password));
        executeSession("match (n) detach delete n");

        StatementResult result = executeSession("match (n) return n");

        assertEquals("nodes should be equal", 0, result.list().size());
    }

    protected StatementResult executeSession(String statement) {
        try (Session session = driver.session()) {
            return session.run(statement);
        }
    }

    @After
    public void tearDown() {
        runner = null;
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testCreateNodeNoReturn() {
        String query = "create (n:nodereturn { name: \"Testing\"})";

        Map<String, String> attributes = clientService.executeQuery(query, new HashMap<>(), EMPTY_CALLBACK);
        assertEquals("1",attributes.get(GraphClientService.LABELS_ADDED));
        assertEquals("1",attributes.get(GraphClientService.NODES_CREATED));
        assertEquals("0",attributes.get(GraphClientService.NODES_DELETED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_CREATED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_DELETED));
        assertEquals("1",attributes.get(GraphClientService.PROPERTIES_SET));
        assertEquals("0",attributes.get(GraphClientService.ROWS_RETURNED));
    }

    @Test
    public void testCreateNodeOnePropertyWithReturn() {
        String query = "create (n { name:'abc' }) return n.name";

        final List<Map<String, Object>> result = new ArrayList<>();
        Map<String, String> attributes = clientService.executeQuery(query, new HashMap<>(), (record, hasMore) -> result.add(record));
        assertEquals("0",attributes.get(GraphClientService.LABELS_ADDED));
        assertEquals("1",attributes.get(GraphClientService.NODES_CREATED));
        assertEquals("0",attributes.get(GraphClientService.NODES_DELETED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_CREATED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_DELETED));
        assertEquals("1",attributes.get(GraphClientService.PROPERTIES_SET));
        assertEquals("1",attributes.get(GraphClientService.ROWS_RETURNED));
        assertEquals(1, result.size());
        assertEquals("abc", result.get(0).get("n.name"));
    }

    @Test
    public void testCreateNodeTwoPropertyOneLabelWithReturn() {
        String query = "create (n:Person { name:'abc', age : 1 }) return n.name, n.age";

        final List<Map<String, Object>> result = new ArrayList<>();
        Map<String, String> attributes = clientService.executeQuery(query, new HashMap<>(), (record, hasMore) -> {
            result.add(record);
        });

        assertEquals("1",attributes.get(GraphClientService.LABELS_ADDED));
        assertEquals("1",attributes.get(GraphClientService.NODES_CREATED));
        assertEquals("0",attributes.get(GraphClientService.NODES_DELETED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_CREATED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_DELETED));
        assertEquals("2",attributes.get(GraphClientService.PROPERTIES_SET));
        assertEquals("1",attributes.get(GraphClientService.ROWS_RETURNED));
        assertEquals(1, result.size());
        assertEquals("abc", result.get(0).get("n.name"));
        assertEquals(1l, result.get(0).get("n.age"));
    }

    @Test
    public void testCreateTwoNodeTwoPropertyOneRelationshipWithReturn() {
        String query = "create (m:Person { name:'abc', age : 1 }) create (n:Person { name : 'pqr'}) create (m)-[r:hello]->(n) return m.name, n.name, type(r)";

        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, String> attributes = clientService.executeQuery(query, new HashMap<>(), ((record, isMore) -> result.add(record)));
        assertEquals("2",attributes.get(GraphClientService.LABELS_ADDED));
        assertEquals("2",attributes.get(GraphClientService.NODES_CREATED));
        assertEquals("0",attributes.get(GraphClientService.NODES_DELETED));
        assertEquals("1",attributes.get(GraphClientService.RELATIONS_CREATED));
        assertEquals("0",attributes.get(GraphClientService.RELATIONS_DELETED));
        assertEquals("3",attributes.get(GraphClientService.PROPERTIES_SET));
        assertEquals("1",attributes.get(GraphClientService.ROWS_RETURNED));
        assertEquals(1, result.size());
        assertEquals("abc", result.get(0).get("m.name"));
        assertEquals("pqr", result.get(0).get("n.name"));
        assertEquals("hello", result.get(0).get("type(r)"));
    }
}