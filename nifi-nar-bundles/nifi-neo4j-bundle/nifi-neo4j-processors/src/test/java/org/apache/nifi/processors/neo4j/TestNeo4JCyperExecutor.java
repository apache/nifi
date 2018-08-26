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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Neo4J Cypher unit tests.
 */
public class TestNeo4JCyperExecutor {
    protected TestRunner runner;
    protected Driver driver;
    protected String neo4jUrl = "bolt://localhost:7687";
    protected String user = "neo4j";
    protected String password = "admin";

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    protected Driver mockDriver;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    protected StatementResult mockStatementResult;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    protected ResultSummary mockResultSummary;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void setUp() throws Exception {
        Neo4JCypherExecutor mockExecutor = new Neo4JCypherExecutor() {

            @Override
            protected StatementResult executeQuery(String query) {
                return mockStatementResult;
            }

            @Override
            protected Driver getNeo4JDriver() {
                return mockDriver;
            }

            @Override
            protected Driver getDriver(ProcessContext context) {
                connectionUrl = "testurl";
                return mockDriver;
            }
        };

        runner = TestRunners.newTestRunner(mockExecutor);
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_URL, neo4jUrl);
        runner.setProperty(AbstractNeo4JCypherExecutor.USERNAME, user);
        runner.setProperty(AbstractNeo4JCypherExecutor.PASSWORD, password);
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "match (n) return n");

        Mockito.when(mockDriver.session().run(Mockito.anyString())).thenReturn(mockStatementResult);
        Mockito.when(mockStatementResult.list()).thenReturn(new ArrayList<Record>());
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
        mockDriver = null;
        mockResultSummary = null;
        mockStatementResult = null;
    }

    @Test
    public void testTestLoadBalancingStrategy() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.LOAD_BALANCING_STRATEGY, AbstractNeo4JCypherExecutor.LOAD_BALANCING_STRATEGY_ROUND_ROBIN.getValue());
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.LOAD_BALANCING_STRATEGY, AbstractNeo4JCypherExecutor.LOAD_BALANCING_STRATEGY_LEAST_CONNECTED.getValue());
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.LOAD_BALANCING_STRATEGY, "BadValue");
        runner.assertNotValid();
    }

    @Test
    public void testTestConnectionTimeout() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_TIMEOUT, "0 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_TIMEOUT, "1 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_TIMEOUT, "-1 seconds");
        runner.assertNotValid();
    }

    @Test
    public void testTestIdleTimeBeforeConnectionTest() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.IDLE_TIME_BEFORE_CONNECTION_TEST, "0 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.IDLE_TIME_BEFORE_CONNECTION_TEST, "1 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.IDLE_TIME_BEFORE_CONNECTION_TEST, "-1 seconds");
        runner.assertNotValid();
    }

    @Test
    public void testEncryption() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.ENCRYPTION, "true");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.ENCRYPTION, "false");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.ENCRYPTION, "bad");
        runner.assertNotValid();
    }

    @Test
    public void testTestMaxConnectionAcquisitionTime() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_ACQUISITION_TIMEOUT, "0 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_ACQUISITION_TIMEOUT, "1 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_ACQUISITION_TIMEOUT, "-1 seconds");
        runner.assertNotValid();
    }

    @Test
    public void testTestMaxConnectionLifeTime() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_LIFETIME, "0 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_LIFETIME, "1 seconds");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_LIFETIME, "-1 seconds");
        runner.assertNotValid();
    }

    @Test
    public void testTestMaxConnectionPoolSize() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_POOL_SIZE, "1");
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_POOL_SIZE, "-1");
        runner.assertNotValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.MAX_CONNECTION_POOL_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testTestTrustStrategy() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.TRUST_STRATEGY, AbstractNeo4JCypherExecutor.TRUST_ALL_CERTIFICATES.getValue());
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.TRUST_STRATEGY, AbstractNeo4JCypherExecutor.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.getValue());
        runner.assertNotValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES_FILE, File.createTempFile("temp", "tmp").getAbsolutePath());
        runner.assertValid();
        runner.setProperty(AbstractNeo4JCypherExecutor.TRUST_STRATEGY, "BadValue");
        runner.assertNotValid();
    }

    @Test
    public void testCreateNodeNoReturn() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "create (n)");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_SUCCESS);
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.LABELS_ADDED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.NODES_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_CREATED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.RELATIONS_DELETED));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.PROPERTIES_SET));
        assertEquals("0",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ROWS_RETURNED));
        flowFiles.get(0).assertContentEquals("[]".getBytes(Charset.defaultCharset()));
    }

    @Test(expected=AssertionError.class)
    public void testGetDriverThrowsException() throws Exception {
        Neo4JCypherExecutor mockExecutor = new Neo4JCypherExecutor() {

            @Override
            protected StatementResult executeQuery(String query) {
                return mockStatementResult;
            }

            @Override
            protected Driver getNeo4JDriver() {
                return mockDriver;
            }

            @Override
            protected Driver getDriver(ProcessContext context) {
                throw new RuntimeException("RuntimeException");
            }
        };

        runner = TestRunners.newTestRunner(mockExecutor);
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_URL, neo4jUrl);
        runner.setProperty(AbstractNeo4JCypherExecutor.USERNAME, user);
        runner.setProperty(AbstractNeo4JCypherExecutor.PASSWORD, password);
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "match (n) return n");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
    }

    @Test
    public void testExecuteQueryThrowsException() throws Exception {
        Neo4JCypherExecutor mockExecutor = new Neo4JCypherExecutor() {

            @Override
            protected StatementResult executeQuery(String query) {
                throw new RuntimeException("QueryException");
            }

            @Override
            protected Driver getNeo4JDriver() {
                return mockDriver;
            }

            @Override
            protected Driver getDriver(ProcessContext context) {
                return mockDriver;
            }
        };

        runner = TestRunners.newTestRunner(mockExecutor);
        runner.setProperty(AbstractNeo4JCypherExecutor.CONNECTION_URL, neo4jUrl);
        runner.setProperty(AbstractNeo4JCypherExecutor.USERNAME, user);
        runner.setProperty(AbstractNeo4JCypherExecutor.PASSWORD, password);
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "match (n) return n");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(Neo4JCypherExecutor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(Neo4JCypherExecutor.REL_FAILURE);
        assertEquals("QueryException",flowFiles.get(0).getAttribute(Neo4JCypherExecutor.ERROR_MESSAGE));
    }

    @Test
    public void testEmptyQuery() throws Exception {
        runner.setProperty(AbstractNeo4JCypherExecutor.QUERY, "");
        runner.assertNotValid();
    }
}