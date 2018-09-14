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
package org.apache.nifi.processors.neo4j;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Neo4J Cypher integration tests.  Please set the neo4j url, user and password according to your setup.
 * The steps to setup neo4j are
 * <ol>
 *   <li> Install Neo4J</li>
 *      <code>brew install neo4j</code>
 *   <li>Setup neo4j</li>
 * <code>neo4j start</code>
 *    <li>Log into cypher shell using default username/password - neo4j/neo4j</li>
 * <code>cypher-shell</code>
 *    <li>Changel password to admin</li>
 * <code>CALL dbms.changePassword('admin')</code>
 *    <li> Restart neo4j</li>
 * <code>neo4j restart</code>
 *    <li>Log into cypher shell using new password (admin)</li>
 * <code>cypher-shell</code>
 * </ol>
 */
public class ITNeo4JCyperExecutor {
    protected TestRunner runner;
    protected Driver driver;
    protected String neo4jUrl = "bolt://localhost:7687";
    protected String user = "neo4j";
    protected String password = "admin";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(Neo4JCypherExecutor.class);
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_URL, neo4jUrl);
        runner.setProperty(AbstractNeo4JCypherExecutor.USERNAME, user);
        runner.setProperty(AbstractNeo4JCypherExecutor.PASSWORD, password);
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "match (n) return n");
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
    public void tearDown() throws Exception {
        runner = null;
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testCreateNodeNoReturn() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "create (n)");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(0).assertContentEquals("[]".getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testCreateNodeOnePropertyWithReturn() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "create (n { name:'abc' }) return n.name");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(0).assertContentEquals("[{\"n.name\":\"abc\"}]".getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testCreateNodeTwoPropertyOneLabelWithReturn() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "create (n:Person { name:'abc', age : 1 }) return n.name, n.age");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("2",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(0).assertContentEquals("[{\"n.name\":\"abc\",\"n.age\":1}]".getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testCreateTwoNodeTwoPropertyOneRelationshipWithReturn() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY,
            "create (m:Person { name:'abc', age : 1 }) create (n:Person { name : 'pqr'}) create (m)-[r:hello]->(n) return m.name, n.name, type(r)");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("2",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("2",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("3",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(0).assertContentEquals("[{\"m.name\":\"abc\",\"n.name\":\"pqr\",\"type(r)\":\"hello\"}]".getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testCreateNodeOnePropertyWithReturnWithEL() throws Exception {
        String query = "create (n { name:'abc' }) return n.name";

        byte [] bytes = new byte [] {};
        Map<String,String> properties = new HashMap<>();
        properties.put("query",query);

        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "${query}");
        runner.enqueue(bytes, properties);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(0).assertContentEquals("[{\"n.name\":\"abc\"}]".getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testCreateTwoNodesWithReturn() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "create (m:Person { name:'abc'}) create (n:Person { name : 'pqr'}) return n.name, m.name");

        runner.enqueue(new byte[] {});
        runner.run(1,false,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);

        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "match (n) return n.name");
        runner.enqueue(new byte[] {});
        runner.run(1,true,true);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("2",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("2",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("2",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("1",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));

        assertEquals("0",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("0",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("0",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("2",flowFiles.get(1).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(1).assertContentEquals("[{\"n.name\":\"abc\"},{\"n.name\":\"pqr\"}]".getBytes(Charset.defaultCharset()));
    }
}