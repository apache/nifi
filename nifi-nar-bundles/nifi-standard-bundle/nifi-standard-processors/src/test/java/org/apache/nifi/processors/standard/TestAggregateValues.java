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
package org.apache.nifi.processors.standard;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


/**
 * Unit tests for AggregateValues processor.
 */
public class TestAggregateValues {

    AggregateValues processor;
    TestRunner runner;

    @Before
    public void setUp() throws Exception {
        processor = new AggregateValues();
        runner = TestRunners.newTestRunner(processor);
    }


    ////////////////////////
    // JSON tests
    ////////////////////////
    @Test
    public void testCountJson() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "$.a");
        addJsonBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testCountJsonBadPath() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "$.b");
        addJsonBatch(runner, 1);
        runner.run(1, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 0);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 1);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 0);
    }

    @Test
    public void testCountJsonNullValue() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "$.n");
        addJsonBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("2", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testMaxJsonStrings() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.MAX);
        runner.setProperty(AggregateValues.PATH, "$.a");
        addJsonBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("Hello4", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.MAX, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testMaxJsonFloats() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.MAX);
        runner.setProperty(AggregateValues.PATH, "$.f");
        addJsonBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("4.0", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.MAX, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testMinJson() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.MIN);
        runner.setProperty(AggregateValues.PATH, "$.c");
        addJsonBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("1", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.MIN, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testSumJsonDoubles() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.SUM);
        runner.setProperty(AggregateValues.PATH, "$.d");
        addJsonBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("10.0", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.SUM, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testAvgJsonInts() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.AVG);
        runner.setProperty(AggregateValues.PATH, "$.c");
        addJsonBatch(runner, 4);
        runner.run(4, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 4);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("2.5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.AVG, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testAvgJsonFloats() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.AVG);
        runner.setProperty(AggregateValues.PATH, "$.f");
        addJsonBatch(runner, 4);
        runner.run(4, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 4);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("1.5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.AVG, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testConcatJsonInts() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.CONCAT);
        runner.setProperty(AggregateValues.PATH, "$.c");
        addJsonBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 0);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 2);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 0);
    }

    @Test
    public void testConcatJsonStringsNoSeparator() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.CONCAT);
        runner.setProperty(AggregateValues.PATH, "$.a");
        addJsonBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("Hello0Hello1", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.CONCAT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testConcatJsonStringsWithSeparator() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.CONCAT);
        runner.setProperty(AggregateValues.OPERATION_PARAM, ", ");
        runner.setProperty(AggregateValues.PATH, "$.a");
        addJsonBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("Hello0, Hello1", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.CONCAT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    private void addJsonBatch(TestRunner runner, int numFlowFiles) {
        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < numFlowFiles; i++) {
            final int index = i;
            String sb = "{\"a\": \"Hello" + index + "\", \"c\": " + (index + 1) + ", \"d\": " + (double) index + ", \"f\": " + (float) index + ", \"n\": null}";
            runner.enqueue(sb, new HashMap<String, String>() {{
                put(CoreAttributes.MIME_TYPE.key(), AggregateValues.JSON_MIME_TYPE);
                put(AggregateValues.FRAGMENT_ID, uuid);
                put(AggregateValues.FRAGMENT_INDEX, Integer.toString(index));
                put(AggregateValues.FRAGMENT_COUNT, Integer.toString(numFlowFiles));
            }});
        }
    }

    ////////////////////////
    // XML tests
    ////////////////////////
    @Test
    public void testCountXml() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "//a");
        addXmlBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testCountXmlBadPath() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "//b");
        addXmlBatch(runner, 1);
        runner.run(1, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 0);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 1);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 0);
    }

    @Test
    public void testCountXmlNullValue() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "//n");
        addXmlBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("2", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testMaxXmlStrings() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.MAX);
        runner.setProperty(AggregateValues.PATH, "//a");
        addXmlBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("Hello4", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.MAX, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testConcatXmlAttribute() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.CONCAT);
        runner.setProperty(AggregateValues.PATH, "//n/@id");
        addXmlBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("XXXXX", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.CONCAT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testAvgXmlFloats() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.AVG);
        runner.setProperty(AggregateValues.PATH, "//f");
        addXmlBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("0.5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.AVG, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    private void addXmlBatch(TestRunner runner, int numFlowFiles) {
        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < numFlowFiles; i++) {
            final int index = i;
            String sb = "<root><a>Hello" + index + "</a><c>"+(index + 1) + "</c><d>" + (double) index + "</d><f>" + (float) index + "</f><n id=\"X\">null</n></root>";
            runner.enqueue(sb, new HashMap<String, String>() {{
                put(CoreAttributes.MIME_TYPE.key(), AggregateValues.XML_MIME_TYPE);
                put(AggregateValues.FRAGMENT_ID, uuid);
                put(AggregateValues.FRAGMENT_INDEX, Integer.toString(index));
                put(AggregateValues.FRAGMENT_COUNT, Integer.toString(numFlowFiles));
            }});
        }
    }

    ////////////////////////
    // CSV tests
    ////////////////////////
    @Test
    public void testCountCsv() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "0");
        addCsvBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testCountCsvBadPath() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "Second Column");
        addCsvBatch(runner, 1);
        runner.run(1, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 0);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 1);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 0);
    }

    @Test
    public void testCountCsvNullValue() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "4");
        addCsvBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("2", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testCountCsvLastColumn() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "7");
        addCsvBatch(runner, 1);
        runner.run(1, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("1", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.COUNT, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testCountCsvPastLastColumn() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.COUNT);
        runner.setProperty(AggregateValues.PATH, "8");
        addCsvBatch(runner, 1);
        runner.run(1, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 0);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 1);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 0);
    }

    @Test
    public void testMaxCsvStrings() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.MAX);
        runner.setProperty(AggregateValues.PATH, "0");
        addCsvBatch(runner, 5);
        runner.run(5, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 5);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("Hello4", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.MAX, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    @Test
    public void testAvgCsvFloats() {
        runner.setProperty(AggregateValues.OPERATION, AggregateValues.AVG);
        runner.setProperty(AggregateValues.PATH, "3");
        addCsvBatch(runner, 2);
        runner.run(2, true, true);
        runner.assertTransferCount(AggregateValues.REL_AGGREGATE, 1);
        runner.assertTransferCount(AggregateValues.REL_FAILURE, 0);
        runner.assertTransferCount(AggregateValues.REL_ORIGINAL, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AggregateValues.REL_AGGREGATE).get(0);
        assertEquals("0.5", flowFile.getAttribute(AggregateValues.AGGREGATE_VALUE));
        assertEquals(AggregateValues.AVG, flowFile.getAttribute(AggregateValues.AGGREGATE_OPERATION));
    }

    private void addCsvBatch(TestRunner runner, int numFlowFiles) {
        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < numFlowFiles; i++) {
            final int index = i;
            String sb = "Hello" + index + ","+(index + 1) + "," + (double) index + "," + (float) index + ",,x,";
            runner.enqueue(sb, new HashMap<String, String>() {{
                put(CoreAttributes.MIME_TYPE.key(), AggregateValues.CSV_MIME_TYPE);
                put(AggregateValues.FRAGMENT_ID, uuid);
                put(AggregateValues.FRAGMENT_INDEX, Integer.toString(index));
                put(AggregateValues.FRAGMENT_COUNT, Integer.toString(numFlowFiles));
            }});
        }
    }


}