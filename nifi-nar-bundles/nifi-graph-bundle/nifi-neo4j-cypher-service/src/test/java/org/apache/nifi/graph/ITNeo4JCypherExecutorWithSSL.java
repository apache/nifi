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

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.graph.Neo4JCypherClientService.CONNECTION_URL;
import static org.apache.nifi.graph.Neo4JCypherClientService.PASSWORD;
import static org.apache.nifi.graph.Neo4JCypherClientService.SSL_TRUST_STORE_FILE;
import static org.apache.nifi.graph.Neo4JCypherClientService.USERNAME;
import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.Config.TrustStrategy.trustCustomCertificateSignedBy;

/**
 * Neo4J Cypher integration tests.
 */
@EnabledIfSystemProperty(named = "neo4j.ssl.test", matches = "true")
public class ITNeo4JCypherExecutorWithSSL {
    protected TestRunner runner;
    protected Driver driver;
    protected String user = "neo4j";
    protected String password = "testing1234";

    private GraphClientService clientService;
    private GraphQueryResultCallback EMPTY_CALLBACK = (record, hasMore) -> {};

    @BeforeEach
    public void setUp() throws Exception {
        String boltUrl = "bolt://localhost:7687";
        String chainFile = "src/test/resources/neo4j/ssl/bolt/public.crt";
        clientService = new Neo4JCypherClientService();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, CONNECTION_URL, boltUrl);
        runner.setProperty(clientService, USERNAME, user);
        runner.setProperty(clientService, PASSWORD, password);
        runner.setProperty(clientService, SSL_TRUST_STORE_FILE,
                chainFile);
        runner.enableControllerService(clientService);
        Config config = Config.builder()
                .withEncryption()
                .withTrustStrategy(trustCustomCertificateSignedBy(new File(chainFile)))
                .build();
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic(user, password), config);
        executeSession("match (n) detach delete n");

        List<Record> result = executeSession("match (n) return n");

        assertEquals("nodes should be equal", 0, result.size());
    }

    protected List<Record> executeSession(String statement) {
        try (Session session = driver.session()) {
            return session.run(statement).list();
        }
    }

    @AfterEach
    public void tearDown() {
        runner = null;
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testQueryWithSSLEnabled() {
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
    public void testPropertyValidation() throws Exception {
        String boltUrl = "bolt+s://localhost:7687";
        String chainFile = "src/test/resources/neo4j/ssl/bolt/public.crt";
        clientService = new Neo4JCypherClientService();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, CONNECTION_URL, boltUrl);
        runner.setProperty(clientService, USERNAME, user);
        runner.setProperty(clientService, PASSWORD, password);
        runner.setProperty(clientService, SSL_TRUST_STORE_FILE,
                chainFile);
        runner.assertNotValid();
    }
}