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

import java.util.List;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for InfluxDB. Please ensure that the InfluxDB is running
 * on local host with default port and has database test with table test. Please set user
 * and password if applicable before running the integration tests.
 */
public class ITPutInfluxDB extends AbstractITInfluxDB {

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutInfluxDB.class);
        initializeRunner();
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL,PutInfluxDB.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDB.RETENTION_POLICY, DEFAULT_RETENTION_POLICY);
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
        initInfluxDB();
    }

    @Test
    public void testValidSinglePoint() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 ";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals(null, flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals(1, result.getResults().iterator().next().getSeries().size(), "size should be same");
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals(1, values.size(), "size should be same");
   }

    @Test
    public void testValidSinglePointWithTime() {
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals(null, result.getResults().iterator().next().getSeries(), "Should have no results");
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals(null, flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals(1, result.getResults().iterator().next().getSeries().size(), "size should be same");
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals(1, values.size(), "size should be same");
    }

    @Test
    public void testValidSinglePointWithTimeAndUrlExpression() {
        runner.setVariable("influxDBUrl", "http://localhost:8086");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "${influxDBUrl}");
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals(null, result.getResults().iterator().next().getSeries(), "Should have no results");
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals(null, flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals(1, result.getResults().iterator().next().getSeries().size(), "size should be same");
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals(1, values.size(), "size should be same");
   }

    @Test
    public void testValidSinglePointWithUsernameEL() {
        runner.setVariable("influxdb.username", "admin");
        runner.setProperty(PutInfluxDB.USERNAME, "${influxdb.username}");
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals(null, result.getResults().iterator().next().getSeries(), "Should have no results");
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
   }

    @Test
    public void testValidSinglePointWithPasswordEL() {
        runner.setVariable("influxdb.password", "admin");
        runner.setProperty(PutInfluxDB.PASSWORD, "${influxdb.password}");
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals(null, result.getResults().iterator().next().getSeries(), "Should have no results");
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
   }

   @Test
    public void testValidTwoPointWithSameMeasurement() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6" + System.lineSeparator()
                + "water,country=US,city=nyc rain=2,humidity=0.7" + System.lineSeparator();
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals(null, flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals(1, result.getResults().iterator().next().getSeries().size(), "size should be same");
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals(2, values.size(), "size should be same");
    }

    @Test
    public void testValidTwoPointWithSameMeasurementBadFormat() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6" + System.lineSeparator()
                + "water,country=US,city=nyc,rain=2,humidity=0.7" + System.lineSeparator();
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_FAILURE);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals("{\"error\":\"partial write: unable to parse 'water,country=US,city=nyc,rain=2,humidity=0.7': missing fields dropped=0\"}\n",
            flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals(1, result.getResults().iterator().next().getSeries().size(), "size should be same");
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals(1, values.size(), "size should be same");
    }

    @Test
    public void testValidTwoPointWithDifferentMeasurement() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6" + System.lineSeparator()
                + "testm,country=US,city=chicago rain=10,humidity=0.9" + System.lineSeparator();
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);
        assertEquals(1, flowFiles.size(), "Value should be equal");
        assertEquals(null, flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Value should be equal");
        QueryResult result = influxDB.query(new Query("select * from water, testm", dbName));
        assertEquals(2, result.getResults().iterator().next().getSeries().size(), "size should be same");
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals(1, values.size(), "size should be same");
    }
}