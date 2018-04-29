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
package org.apache.nifi.processors.influxdb;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestExecutetInfluxDBQuery {
    private TestRunner runner;
    private ExecuteInfluxDBQuery mockExecuteInfluxDBQuery;

    @Before
    public void setUp() throws Exception {
        mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout) {
                return null;
            }

            @Override
            protected List<QueryResult> executeQuery(ProcessContext context, String database, String query, TimeUnit timeunit, int chunkSize) {
                return null;
            }

        };
        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        runner.setProperty(ExecuteInfluxDBQuery.DB_NAME, "test");
        runner.setProperty(ExecuteInfluxDBQuery.USERNAME, "user");
        runner.setProperty(ExecuteInfluxDBQuery.PASSWORD, "password");
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(ExecuteInfluxDBQuery.CHARSET, "UTF-8");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testDefaultValid() {
        runner.assertValid();
    }

    @Test
    public void testQueryThrowsRuntimeException() {
        mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout) {
                return null;
            }

            @Override
            protected List<QueryResult> executeQuery(ProcessContext context, String database, String query, TimeUnit timeunit, int chunkSize) {
                throw new RuntimeException("runtime exception");
            }

        };
        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        runner.setProperty(ExecuteInfluxDBQuery.DB_NAME, "test");
        runner.setProperty(ExecuteInfluxDBQuery.USERNAME, "u1");
        runner.setProperty(ExecuteInfluxDBQuery.PASSWORD, "p1");
        runner.setProperty(ExecuteInfluxDBQuery.CHARSET, "UTF-8");
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_URL, "http://dbUrl");
        runner.assertValid();

        byte [] bytes = "select * from /.*/".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE),"runtime exception");
    }

    @Test
    public void testQueryThrowsRuntimeExceptionWithSocketTimeoutException() {
        mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout) {
                return null;
            }

            @Override
            protected List<QueryResult> executeQuery(ProcessContext context, String database, String query, TimeUnit timeunit, int chunkSize) {
                throw new RuntimeException("runtime exception", new SocketTimeoutException("timeout"));
            }

        };
        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        runner.setProperty(ExecuteInfluxDBQuery.DB_NAME, "test");
        runner.setProperty(ExecuteInfluxDBQuery.USERNAME, "u1");
        runner.setProperty(ExecuteInfluxDBQuery.PASSWORD, "p1");
        runner.setProperty(ExecuteInfluxDBQuery.CHARSET, "UTF-8");
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_URL, "http://dbUrl");
        runner.assertValid();

        byte [] bytes = "select * from /.*/".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_RETRY, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_RETRY);

        assertEquals(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE),"runtime exception");
    }

    @Test(expected=ProcessException.class)
    public void testMakingQueryThrowsIOException() throws Throwable {
        mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout) {
                return null;
            }

            @Override
            protected String getQuery(ProcessSession session, Charset charset, FlowFile incomingFlowFile)
                    throws IOException {
                throw new IOException("Test IOException");
            }
        };

        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        runner.setProperty(ExecuteInfluxDBQuery.DB_NAME, "test");
        runner.setProperty(ExecuteInfluxDBQuery.USERNAME, "u1");
        runner.setProperty(ExecuteInfluxDBQuery.PASSWORD, "p1");
        runner.setProperty(ExecuteInfluxDBQuery.CHARSET, "UTF-8");
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_URL, "http://dbUrl");
        runner.assertValid();

        byte [] bytes = "select * from /.*/".getBytes();
        runner.enqueue(bytes);
        try {
            runner.run(1,true,true);
        } catch (AssertionError e) {
            throw e.getCause();
        }
    }

    @Test
    public void testMakeConnectionThrowsRuntimeException() {
        mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout) {
                throw new RuntimeException("testException");
            }

        };
        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        runner.setProperty(ExecuteInfluxDBQuery.DB_NAME, "test");
        runner.setProperty(ExecuteInfluxDBQuery.USERNAME, "u1");
        runner.setProperty(ExecuteInfluxDBQuery.PASSWORD, "p1");
        runner.setProperty(ExecuteInfluxDBQuery.CHARSET, "UTF-8");
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_URL, "http://dbUrl");
        runner.assertValid();

        byte [] bytes = "select * from /.*/".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE),"Error while getting connection testException");
    }

    @Test
    public void testTriggerThrowsException() {
        mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected InfluxDB getInfluxDB(ProcessContext context) {
                throw new RuntimeException("testException");
            }
        };
        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        runner.setProperty(ExecuteInfluxDBQuery.DB_NAME, "test");
        runner.setProperty(ExecuteInfluxDBQuery.USERNAME, "u1");
        runner.setProperty(ExecuteInfluxDBQuery.PASSWORD, "p1");
        runner.setProperty(ExecuteInfluxDBQuery.CHARSET, "UTF-8");
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_URL, "http://dbUrl");
        runner.assertValid();

        byte [] bytes = "select * from".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE),"testException");
    }

}