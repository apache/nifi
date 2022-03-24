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

import com.google.gson.Gson;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Integration test for executing InfluxDB queries. Please ensure that the InfluxDB is running
 * on local host with default port and has database test with table test. Please set user
 * and password if applicable before running the integration tests.
 */
public class ITExecuteInfluxDBQuery extends AbstractITInfluxDB {

    protected Gson gson = new Gson();
    @BeforeEach
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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

        String result = new String(flowFiles.get(0).toByteArray());
        QueryResult queryResult = gson.fromJson(new StringReader(result), QueryResult.class);
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        assertEquals("databases", series.getName(), "series name should be same");
        assertEquals("name", series.getColumns().get(0), "series column should be same");
        boolean internal = series.getValues().get(0).stream().anyMatch(o -> o.equals("_internal"));
        assertTrue(internal, "content should contain _internal " + queryResult);
        boolean test = series.getValues().stream().flatMap(i -> ((List<Object>)i).stream()).anyMatch(o -> o.equals("test"));
        assertTrue(test, "content should contain test " + queryResult);
    }

    @Test
    public void testCreateDB() {
        String query = "create database test1";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull(queryResult.getResults(), "QueryResult should not be null");
        assertEquals(1, queryResult.getResults().size(), "results array should be same size");
        assertNull(queryResult.getResults().get(0).getSeries(), "No series");
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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals(null, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull(queryResult.getResults(), "QueryResult should not be null");
        assertEquals(1, queryResult.getResults().size(), "results array should be same size");
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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query.replace("${measurement}", "water"), flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull(queryResult.getResults(), "QueryResult should not be null");
        assertEquals(1, queryResult.getResults().size(), "results array should be same size");
        Series series = queryResult.getResults().get(0).getSeries().get(0);
        validateSeries(series.getName(), series.getColumns(), series.getValues().get(0),"newark",1.0);
    }

    protected void validateSeries(String name, List<String> columns, List<Object> values, String city, double rain) {
        assertEquals("water", name, "Series name should be same");
        assertEquals("time", columns.get(0), "Series columns should be same");
        assertEquals("city", columns.get(1), "Series columns should be same");
        assertEquals("country", columns.get(2), "Series columns should be same");
        assertEquals("humidity", columns.get(3), "Series columns should be same");
        assertEquals("rain", columns.get(4), "Series columns should be same");

        assertEquals(1.50100227485666867E18, values.get(0), "time value should be same");
        assertEquals(city, values.get(1), "city value should be same");
        assertEquals("US", values.get(2), "contry value should be same");
        assertEquals(0.6, values.get(3), "humidity value should be same");
        assertEquals(rain, values.get(4), "rain value should be same");
    }

    @Test
    public void testEmptyFlowFileQuery() {
        String query = "";
        byte [] bytes = query.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDBQuery.REL_FAILURE, 1);
        List<MockFlowFile> flowFilesSuccess = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS);
        assertEquals(0, flowFilesSuccess.size(), "Value should be equal");
        List<MockFlowFile> flowFilesFailure = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);
        assertEquals(1, flowFilesFailure.size(), "Value should be equal");
        assertEquals("FlowFile query is empty and no scheduled query is set", flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        assertNull(flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be null");
    }

    @Test
    public void testNoFlowFileNoScheduledInfluxDBQuery() {
        AssertionError error = assertThrows(AssertionError.class, () -> {
            runner.setIncomingConnection(false);
            runner.run(1, true, true);
        });
        assertEquals(
                "Could not invoke methods annotated with @OnScheduled annotation due to: java.lang.reflect.InvocationTargetException",
                error.getLocalizedMessage(), "Message should be same");
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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");
        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull(queryResult.getResults(), "QueryResult should not be null");
        assertEquals(1, queryResult.getResults().size(), "results array should be same size");
        assertEquals(1, queryResult.getResults().get(0).getSeries().size(), "Series size should be same");
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
       assertEquals(0, flowFilesSuccess.size(), "Value should be equal");
       List<MockFlowFile> flowFilesFailure = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE);
       assertEquals(1, flowFilesFailure.size(), "Value should be equal");
       assertEquals("{\"error\":\"error parsing query: found EOF, expected identifier at line 1, char 15\"}",
           flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE).trim(), "Value should be equal");
       assertEquals(query, flowFilesFailure.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");
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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");
        assertEquals(query, flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY), "Value should be equal");

        QueryResult queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResult.class);
        assertNotNull(queryResult.getResults(), "QueryResult should not be null");
        assertEquals(1, queryResult.getResults().size(), "Result size should be same");
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
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertNull(flowFiles.get(0).getAttribute(ExecuteInfluxDBQuery.INFLUX_DB_ERROR_MESSAGE), "Value should be null");

        List<QueryResult> queryResult = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())), QueryResultListType);

        assertNotNull(queryResult, "QueryResult array should not be null");
        assertEquals(2, queryResult.size(), "QueryResult array size should be equal 2");

        assertEquals(2, chunkSize(queryResult.get(0)), "First chunk should have 2 elements");
        assertEquals(1, chunkSize(queryResult.get(1)), "Second chunk should have 1 elements");
    }

    private int chunkSize(QueryResult queryResult) {
        return queryResult.getResults().get(0).getSeries().get(0).getValues().size();
    }
}