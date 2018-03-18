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
import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for executing InfluxDB queries. Please ensure that the InfluxDB is running
 * on local host with default port and has database test with table test. Please set user
 * and password if applicable before running the integration tests.
 */
public class ITExecuteInfluxDBQuery extends AbstractITInfluxDB {

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ExecuteInfluxDBQuery.class);
        initializeRunner();
        initInfluxDB();
    }

    @Test
    public void testValidSinglePoint() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1501002274856668652";
        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);
        String query = "select * from water";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
        flowFiles.get(0).assertContentEquals(
            "{\"results\":[{\"series\":[{\"name\":\"water\",\"columns\":[\"time\",\"city\",\"country\",\"humidity\",\"rain\"],\"values\":"
            + "[[1.50100227485666867E18,\"newark\",\"US\",0.6,1.0]]}]}]}");
    }

    @Test
    public void testEmptyQuery() {
        String query = "";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);
        List<MockFlowFile> flowFilesSuccess = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 0, flowFilesSuccess.size());
        List<MockFlowFile> flowFilesFailure = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);
        assertEquals("Value should be equal", 1, flowFilesFailure.size());
        assertEquals("Value should be equal","Empty query size is 0", flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",null, flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
    }

    @Test
    public void testValidTwoPoints() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1501002274856668652" +
            System.lineSeparator() +
            "water,country=US,city=nyc rain=2,humidity=0.6 1501002274856668652";
        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);
        String query = "select * from water";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
        flowFiles.get(0).assertContentEquals(
                "{\"results\":[{\"series\":[{\"name\":\"water\",\"columns\":[\"time\",\"city\",\"country\",\"humidity\",\"rain\"],\"values\":"
                + "[[1.50100227485666867E18,\"newark\",\"US\",0.6,1.0],[1.50100227485666867E18,\"nyc\",\"US\",0.6,2.0]]}]}]}");
    }

    @Test
    public void testMalformedQuery() {
       String query = "select * from";
       byte [] bytes = query.getBytes();
       runner.enqueue(bytes);
       runner.run(1,true,true);
       runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);
       List<MockFlowFile> flowFilesSuccess = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
       assertEquals("Value should be equal", 0, flowFilesSuccess.size());
       List<MockFlowFile> flowFilesFailure = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);
       assertEquals("Value should be equal", 1, flowFilesFailure.size());
       assertEquals("Value should be equal","{\"error\":\"error parsing query: found EOF, expected identifier at line 1, char 15\"}",
           flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE).trim());
       assertEquals("Value should be equal",query, flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
    }

    @Test
    public void testQueryResultHasError() {
        ExecuteInfluxDBQuery mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected QueryResult executeQuery(ProcessContext context, String database, String query, TimeUnit timeunit) {
                QueryResult result = super.executeQuery(context, database, query, timeunit);
                result.setError("Test Error");
                return result;
            }

        };
        runner = TestRunners.newTestRunner(mockExecuteInfluxDBQuery);
        initializeRunner();

        byte [] bytes = "select * from /.*/".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);

        assertEquals("Test Error",flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void testValidSameTwoPoints() {
        String message = "water,country=US,city=nyc rain=1,humidity=0.6 1501002274856668652" +
            System.lineSeparator() +
            "water,country=US,city=nyc rain=1,humidity=0.6 1501002274856668652";
        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);
        String query = "select * from water";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
        flowFiles.get(0).assertContentEquals(
            "{\"results\":[{\"series\":[{\"name\":\"water\",\"columns\":[\"time\",\"city\",\"country\",\"humidity\",\"rain\"],\"values\":"
            + "[[1.50100227485666867E18,\"nyc\",\"US\",0.6,1.0]]}]}]}");
    }

    @Test
    public void testValidTwoPointsUrlEL() {
        runner.setVariable("influxDBUrl", "http://localhost:8086");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "${influxDBUrl}");
        testValidTwoPoints();
    }
}