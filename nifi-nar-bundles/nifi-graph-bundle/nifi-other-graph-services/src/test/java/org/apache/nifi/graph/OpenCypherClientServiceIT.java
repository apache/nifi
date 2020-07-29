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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.opencypher.gremlin.neo4j.driver.GremlinDatabase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * To run this, setup JanusGraph using just the BerkeleyJE configuration for the server.
 *
 * 1. Documentation for configuring JanusGraph: https://docs.janusgraph.org/latest/configuration.html
 * 2. Documentation for setting up the Janus Server including adding both REST and WebSocket support:
 *    https://docs.janusgraph.org/latest/server.html
 *
 * Then follow these steps with JanusGraph to install OpenCypher support:
 *
 * 1. Add support to the Janus console: https://github.com/opencypher/cypher-for-gremlin/tree/master/tinkerpop/cypher-gremlin-console-plugin
 * 2. Add support to the Janus server: https://github.com/opencypher/cypher-for-gremlin/tree/master/tinkerpop/cypher-gremlin-server-plugin
 */
public class OpenCypherClientServiceIT {
    TestRunner runner;
    GraphClientService service;
    private Driver driver;

    @Before
    public void before() throws Exception {
        service = new OpenCypherClientService();
        runner = TestRunners.newTestRunner(MockProcessor.class);
        runner.addControllerService("clientService", service);
        runner.setProperty(service, AbstractTinkerpopClientService.CONTACT_POINTS, "localhost");
        runner.setProperty(MockProcessor.CLIENT, "clientService");
        runner.enableControllerService(service);
        runner.assertValid();

        Assert.assertEquals("gremlin://localhost:8182/gremlin", service.getTransitUrl());

        driver = GremlinDatabase.driver("//localhost:8182");
        executeSession("MATCH (n) detach delete n");
        executeSession("CREATE (rover:dog { name: \"Rover\"})");
        executeSession("CREATE (fido:dog { name: \"Fido\"})");
        executeSession("MATCH (fido:dog) WHERE fido.name = \"Rover\" " +
                "MATCH (rover:dog) WHERE rover.name = \"Rover\" " +
                "CREATE (rover)-[:chases]->(fido)");
    }

    @After
    public void after() {
        executeSession("MATCH (n) DETACH DELETE n");
    }

    protected StatementResult executeSession(String statement) {
        try (Session session = driver.session()) {
            return session.run(statement);
        }
    }

    @Test
    public void testBasicQuery() {
        String query = "MATCH (n) RETURN n";

        List<Map<String, Object>> results = new ArrayList<>();
        Map<String, String> attributes = service.executeQuery(query, new HashMap<>(), (record, hasMore) -> results.add(record));
        assertNotNull(attributes);
        assertEquals(7, attributes.size());
        assertEquals(2, results.size());
    }
}
