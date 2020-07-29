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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.junit.Assert;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;



/**
 * Integration test for executing InfluxDB queries. Please ensure that the InfluxDB is running
 * on local host with default port and has database test with table test. Please set user
 * and password if applicable before running the integration tests.
 */
public class ITExecuteInfluxDBQuery extends AbstractITInfluxDB {

    protected Gson gson = new Gson();
    @Before
    public void setUp() throws Exception {
        initInfluxDB();
        runner = TestRunners.newTestRunner(ExecuteInfluxDBQuery.class);
        initializeRunner();
        runner.setVariable("influxDBUrl", "http://localhost:8086");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "${influxDBUrl}");
    }

    @Test
    public void testValidScheduleQueryWithNoIncoming() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1501002274856668652";
        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);

        String query = "select * from water";
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_QUERY, query);

        runner.setIncomingConnection(false);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series.getName(), series.getColumns(), series.getValues().get(0),"newark",1.0);
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
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series.getName(), series.getColumns(), series.getValues().get(0),"newark",1.0);
    }

    @Test
    public void testShowDatabases() {
        String query = "show databases";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        String result = new String(flowFiles.get(0).toByteArray());
        QueryResult queryResult = gson.fromJson(new StringReader(result), QueryResult.class);
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        assertEquals("series name should be same", "databases", series.getName());
        assertEquals("series column should be same", "name", series.getColumns().get(0));
        boolean internal = series.getValues().get(0).stream().anyMatch(o -> o.equals("_internal"));
        Assert.assertTrue("content should contain _internal " + queryResult, internal);
        boolean test = series.getValues().stream().flatMap(i -> ((List<Object>)i).stream()).anyMatch(o -> o.equals("test"));
        Assert.assertTrue("content should contain test " + queryResult, test);
    }

    @Test
    public void testCreateDB() {
        String query = "create database test1";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull("QueryResult should not be null", queryResult.getResults());
        assertEquals("results array should be same size", 1, queryResult.getResults().size());
        assertNull("No series", queryResult.getResults().get(0).getSeries());
    }

    @Test
    public void testEmptyFlowFileQueryWithScheduledQuery() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1501002274856668652";
        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);

        String query = "select * from water";
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_QUERY, query);

        byte [] bytes = new byte [] {};
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull("QueryResult should not be null", queryResult.getResults());
        assertEquals("results array should be same size", 1, queryResult.getResults().size());
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series.getName(), series.getColumns(), series.getValues().get(0),"newark",1.0);
    }

    @Test
    public void testEmptyFlowFileQueryWithScheduledQueryEL() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1501002274856668652";
        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);

        String query = "select * from ${measurement}";
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_QUERY, query);

        byte [] bytes = new byte [] {};
        Map<String,String> properties = new HashMap<>();
        properties.put("measurement","water");
        runner.enqueue(bytes, properties);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertNull("Value should be null",flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query.replace("${measurement}", "water"), flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull("QueryResult should not be null", queryResult.getResults());
        assertEquals("results array should be same size", 1, queryResult.getResults().size());
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series.getName(), series.getColumns(), series.getValues().get(0),"newark",1.0);
    }

    protected void validateSeries(String name, List<String> columns, List<Object> values, String city, double rain) {
        assertEquals("Series name should be same","water", name);
        assertEquals("Series columns should be same","time", columns.get(0));
        assertEquals("Series columns should be same","city", columns.get(1));
        assertEquals("Series columns should be same","country", columns.get(2));
        assertEquals("Series columns should be same","humidity", columns.get(3));
        assertEquals("Series columns should be same","rain", columns.get(4));

        assertEquals("time value should be same", 1.50100227485666867E18, values.get(0));
        assertEquals("city value should be same", city, values.get(1));
        assertEquals("contry value should be same", "US", values.get(2));
        assertEquals("humidity value should be same", 0.6, values.get(3));
        assertEquals("rain value should be same", rain, values.get(4));
    }

    @Test
    public void testEmptyFlowFileQuery() {
        String query = "";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);
        List<MockFlowFile> flowFilesSuccess = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 0, flowFilesSuccess.size());
        List<MockFlowFile> flowFilesFailure = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);
        assertEquals("Value should be equal", 1, flowFilesFailure.size());
        assertEquals("Value should be equal","FlowFile query is empty and no scheduled query is set", flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertNull("Value should be null", flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
    }

    @Test
    public void testNoFlowFileNoScheduledInfluxDBQuery() {
        try {
            runner.setIncomingConnection(false);
            runner.run(1,true,true);
            Assert.fail("Should throw assertion error");
        } catch(AssertionError error) {
            assertEquals("Message should be same",
                "Could not invoke methods annotated with @OnScheduled annotation due to: java.lang.reflect.InvocationTargetException",
                    error.getLocalizedMessage());
        }
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
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));
        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull("QueryResult should not be null", queryResult.getResults());
        assertEquals("results array should be same size", 1, queryResult.getResults().size());
        assertEquals("Series size should be same",1, queryResult.getResults().get(0).getSeries().size());
        Series series1 = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series1.getName(),series1.getColumns(), series1.getValues().get(0),"newark",1.0);

        Series series2 = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series2.getName(),series2.getColumns(), series2.getValues().get(1),"nyc",2.0);
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
    public void testQueryResultHasError() throws Throwable {
        ExecuteInfluxDBQuery mockExecuteInfluxDBQuery = new ExecuteInfluxDBQuery() {
            @Override
            protected List<QueryResult> executeQuery(ProcessContext context, String database, String query, TimeUnit timeunit, int chunkSize) throws InterruptedException{
                List<QueryResult> result = super.executeQuery(context, database, query, timeunit, chunkSize);
                result.get(0).setError("Test Error");
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
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));
        assertEquals("Value should be equal",query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY));

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull("QueryResult should not be null", queryResult.getResults());
        assertEquals("Result size should be same", 1, queryResult.getResults().size());
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series.getName(), series.getColumns(), series.getValues().get(0),"nyc",1.0);
    }

    @Test
    public void testValidTwoPointsUrlEL() {
        runner.setVariable("influxDBUrl", "http://localhost:8086");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "${influxDBUrl}");
        testValidTwoPoints();
    }

    @Test
    public void testChunkedQuery() {
        String message =
                "chunkedQueryTest,country=GB value=1 1524938495000000000" + System.lineSeparator() +
                "chunkedQueryTest,country=PL value=2 1524938505000000000" + System.lineSeparator() +
                "chunkedQueryTest,country=US value=3 1524938505800000000";

        influxDB.write(dbName, DEFAULT_RETENTION_POLICY, InfluxDB.ConsistencyLevel.ONE, message);

        String query = "select * from chunkedQueryTest";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_QUERY_CHUNK_SIZE, "2");
        runner.run(1,true,true);

        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertNull("Value should be null", flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE));

        List<QueryResult> queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResultListType);

        assertNotNull("QueryResult array should not be null", queryResult);
        assertEquals("QueryResult array size should be equal 2", 2, queryResult.size());

        assertEquals("First chunk should have 2 elements",2, chunkSize(queryResult.get(0)));
        assertEquals("Second chunk should have 1 elements",1, chunkSize(queryResult.get(1)));
    }

    private int chunkSize(QueryResult queryResult) {
        return queryResult.getResults().get(0).getSeries().get(0).getValues().size();
    }
}