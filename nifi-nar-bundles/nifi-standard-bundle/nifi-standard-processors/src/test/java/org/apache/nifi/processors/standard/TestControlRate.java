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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.standard.ControlRate.MAX_FLOW_FILES_PER_BATCH;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestControlRate {

    private static final String ONE_SECOND_TIME_PERIOD = "1 s";

    private static final long CURRENT_TIME_INCREMENT = 1100;

    private ConfigurableControlRate controlRate;

    private TestRunner runner;

    @BeforeEach
    public void setRunner() {
        controlRate = new ConfigurableControlRate();
        runner = TestRunners.newTestRunner(controlRate);
    }

    @Test
    public void testRouteToRateExceeded() {
        runner.setProperty(ControlRate.RATE_EXCEEDED_STRATEGY, ControlRate.ROUTE_TO_RATE_EXCEEDED.getValue());
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "10");
        runner.setProperty(ControlRate.TIME_PERIOD, "1 min");
        runner.setProperty(ControlRate.GROUPING_ATTRIBUTE_NAME, "group");

        for (int i = 0; i < 100; i++) {
            final Map<String, String> attributes = Collections.singletonMap("group", Integer.toString(i));
            runner.enqueue("", attributes);
        }

        for (int i = 0; i < 25; i++) {
            final Map<String, String> attributes = Collections.singletonMap("group", "50");
            runner.enqueue("", attributes);
        }

        runner.run();

        // The first 100 should all go to success, as should the next 9 (as that's a total of 10 for group '50').
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 109);

        // The rest should all go to 'rate exceeded'
        runner.assertTransferCount(ControlRate.REL_RATE_EXCEEDED, 16);
        final List<MockFlowFile> exceededFlowFiles = runner.getFlowFilesForRelationship(ControlRate.REL_RATE_EXCEEDED);
        exceededFlowFiles.forEach(ff -> ff.assertAttributeEquals("group", "50"));
    }

    @Test
    public void testLimitExceededThenOtherLimitNotExceeded() {
        // If we have flowfiles queued that have different values for the "Rate Controlled Attribute"
        // and we encounter a FlowFile whose rate should be throttled, we should continue pulling other flowfiles
        // whose rate does not need to be throttled.
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "3");
        runner.setProperty(ControlRate.TIME_PERIOD, "1 min");
        runner.setProperty(ControlRate.GROUPING_ATTRIBUTE_NAME, "group");

        final Map<String, String> group1 = Collections.singletonMap("group", "1");
        final Map<String, String> group2 = Collections.singletonMap("group", "2");

        for (int i = 0; i < 5; i++) {
            runner.enqueue("test data", group1);
        }

        runner.enqueue("test data", group2);

        // Run several times, just to allow the processor to terminate the first poll if it wishes to
        runner.run();

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 4);

        final List<MockFlowFile> output = runner.getFlowFilesForRelationship(ControlRate.REL_SUCCESS);
        assertEquals(3L, output.stream().filter(ff -> ff.getAttribute("group").equals("1")).count());
        assertEquals(1L, output.stream().filter(ff -> ff.getAttribute("group").equals("2")).count());
    }

    @Test
    public void testFileCountRate() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "3");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        runner.enqueue("test data 1");
        runner.enqueue("test data 2");
        runner.enqueue("test data 3");
        runner.enqueue("test data 4");

        runner.run(4, false);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 3);
        runner.clearTransferState();

        runner.run(50, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // we have sent 3 files and after 1 second, we should be able to send the 4th
        incrementCurrentTime();

        runner.run(5);
        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testFileCountWithGrouping() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "2");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);
        runner.setProperty(ControlRate.GROUPING_ATTRIBUTE_NAME, "group");

        createFlowFileWithGroup(runner, "one");
        createFlowFileWithGroup(runner, "two");
        createFlowFileWithGroup(runner, "one");
        createFlowFileWithGroup(runner, "two");
        createFlowFileWithGroup(runner, "one");
        createFlowFileWithGroup(runner, "two");

        runner.run(6, false);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 4);
        runner.clearTransferState();

        runner.run(50, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // we have sent 2 files per group and after 1 second, we should be able to send the remaining 1 file per group
        incrementCurrentTime();
        runner.run(2);
        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 2);
        runner.assertQueueEmpty();
    }

    @Test
    public void testDataSizeRate() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "20 b");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        runner.enqueue("testdata 1");
        runner.enqueue("testdata 2");
        runner.enqueue("testdata 3");
        runner.enqueue("testdata 4");

        runner.run(4, false);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 2);
        runner.clearTransferState();

        runner.run(50, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // we have sent 20 bytes and after 1 second, we should be able to send 20 more
        incrementCurrentTime();
        runner.run(2, false);
        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 2);
        runner.assertQueueEmpty();
    }

    @Test
    public void testViaAttribute() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.ATTRIBUTE_RATE);
        runner.setProperty(ControlRate.RATE_CONTROL_ATTRIBUTE_NAME, "count");
        runner.setProperty(ControlRate.MAX_RATE, "20000");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        createFlowFile(runner, 1000);
        createFlowFile(runner, 3000);
        createFlowFile(runner, 5000);
        createFlowFile(runner, 20000);
        createFlowFile(runner, 1000);

        runner.run(5, false);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 4);
        runner.clearTransferState();

        // at this point, we have sent through 29,000 but our max is 20,000 per second.
        // After 1.45 seconds (29000 / 20000), we should be able to send another 20,000
        runner.run(50, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();
        incrementCurrentTime(1450);

        // at this point, more than TIME_PERIOD 1.0 seconds but less than 1.45 seconds have passed
        runner.run(50, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();
        incrementCurrentTime(600);

        // at this point, more than 1.45 seconds have passed, so we should be able to send another 20,000
        runner.run();
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 1);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testAttributeDoesNotExist() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.ATTRIBUTE_RATE);
        runner.setProperty(ControlRate.RATE_CONTROL_ATTRIBUTE_NAME, "no.such.attribute");
        runner.setProperty(ControlRate.MAX_RATE, "20000");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        createFlowFile(runner, 1000);
        createFlowFile(runner, 3000);
        createFlowFile(runner, 5000);
        createFlowFile(runner, 20000);
        createFlowFile(runner, 1000);

        runner.run(5, false);

        // all flowfiles transfer to failure since throttling attribute is not present
        runner.assertAllFlowFilesTransferred(ControlRate.REL_FAILURE, 5);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testBadAttributeRate() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.ATTRIBUTE_RATE);
        runner.setProperty(ControlRate.RATE_CONTROL_ATTRIBUTE_NAME, "count");
        runner.setProperty(ControlRate.MAX_RATE, "20000");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("count", "bad string");
        runner.enqueue(new byte[0], attributeMap);

        runner.run();
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testBatchLimit() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "5555");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        final int TEST_FILE_COUNT = 1500;

        for (int i = 0; i < TEST_FILE_COUNT; i++) {
            runner.enqueue("test data " + i);
        }

        runner.run(1, false);

        // after 1 run should have MAX_FLOW_FILES_PER_BATCH files transferred and remainder of TEST_FILE_COUNT in queue
        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, MAX_FLOW_FILES_PER_BATCH);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();
        assertEquals(TEST_FILE_COUNT - MAX_FLOW_FILES_PER_BATCH, runner.getQueueSize().getObjectCount());

        runner.run(1, false);

        // after 2 runs should have TEST_FILE_COUNT files transferred and 0 in queue
        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, TEST_FILE_COUNT);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testNonExistingGroupAttribute() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "2");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);
        runner.setProperty(ControlRate.GROUPING_ATTRIBUTE_NAME, "group");

        createFlowFileWithGroup(runner, "one");
        createFlowFile(runner, 1); // no group set on this flow file
        createFlowFileWithGroup(runner, "one");
        createFlowFile(runner, 2); // no group set on this flow file

        runner.run(4, false);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 4);
        runner.assertQueueEmpty();
    }

    @Test
    public void testIncreaseDataRate() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "11 B");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        runner.enqueue("test data 1");
        runner.enqueue("test data 2");
        runner.enqueue("test data 3");
        runner.enqueue("test data 4");
        runner.enqueue("test data 5");
        runner.enqueue("test data 6");

        runner.run(7, true);

        runner.assertTransferCount(ControlRate.REL_SUCCESS, 1);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // Increase rate after stopping processor. Previous count should remain since we are still inside time period
        runner.setProperty(ControlRate.MAX_RATE, "33 B");
        runner.run(7, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 3);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // after 1 second, we should be able to send the up to 3 more flowfiles
        incrementCurrentTime();
        runner.run(7, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 6);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testIncreaseFlowFileRate() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_RATE, "1");
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);

        runner.enqueue("test data 1");
        runner.enqueue("test data 2");
        runner.enqueue("test data 3");
        runner.enqueue("test data 4");
        runner.enqueue("test data 5");
        runner.enqueue("test data 6");

        runner.run(7, true);

        runner.assertTransferCount(ControlRate.REL_SUCCESS, 1);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // Increase rate after stopping processor. Previous count should remain since we are still inside time period
        runner.setProperty(ControlRate.MAX_RATE, "3");
        runner.run(7, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 3);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // after 1 second, we should be able to send the up to 3 more flowfiles
        incrementCurrentTime();
        runner.run(7, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 6);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testDataOrFlowFileCountLimitedByBytes() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_OR_FLOWFILE_RATE);
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);
        // Data rate will throttle before FlowFile count
        runner.setProperty(ControlRate.MAX_DATA_RATE, "22 B");
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "3");

        runner.enqueue("test data 1");
        runner.enqueue("test data 2");
        runner.enqueue("test data 3");

        runner.run(4, false);

        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 2);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();
        runner.clearTransferState();

        runner.run(4, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 0);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();
        // we have sent 22 bytes and after 1 second, we should be able to send 22 more
        incrementCurrentTime(1500);
        runner.run(4, false);
        runner.assertAllFlowFilesTransferred(ControlRate.REL_SUCCESS, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testDataOrFlowFileCountLimitedByCount() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_OR_FLOWFILE_RATE);
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);
        // FlowFile count rate will throttle before data rate
        runner.setProperty(ControlRate.MAX_DATA_RATE, "44 B"); // greater than all flowfiles to be queued
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "1");  // limit to 1 flowfile per second

        runner.enqueue("test data 1");
        runner.enqueue("test data 2");
        runner.enqueue("test data 3");

        runner.run(1, false);

        runner.assertTransferCount(ControlRate.REL_SUCCESS, 1);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        incrementCurrentTime(2000);
        runner.run(1, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 3);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testDataOrFlowFileCountLimitedByBytesThenCount() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_OR_FLOWFILE_RATE);
        runner.setProperty(ControlRate.TIME_PERIOD, ONE_SECOND_TIME_PERIOD);
        // Data rate will throttle before FlowFile count
        runner.setProperty(ControlRate.MAX_DATA_RATE, "22 B");
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "5");

        runner.enqueue("test data 1");
        runner.enqueue("test data 2");
        runner.enqueue("test data 3");
        runner.enqueue("4");
        runner.enqueue("5");
        runner.enqueue("6");
        runner.enqueue("7");
        runner.enqueue("8");

        runner.run(10, false);

        runner.assertTransferCount(ControlRate.REL_SUCCESS, 2);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueNotEmpty();

        // we have sent 2 flowfile and after 1 second, we should be able to send more, now limited by flowfile count
        incrementCurrentTime(1500);
        runner.run(1, false);
        runner.assertTransferCount(ControlRate.REL_SUCCESS, 8);
        runner.assertTransferCount(ControlRate.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }

    @Test
    public void testValidate() {
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_RATE);
        runner.assertNotValid(); // MAX_RATE is not set
        runner.setProperty(ControlRate.MAX_RATE, "1");
        runner.assertNotValid(); // MAX_RATE is not a byte size
        runner.setProperty(ControlRate.MAX_RATE, "1 MB");
        runner.assertValid();
        runner.setProperty(ControlRate.MAX_DATA_RATE, "1 MB");
        runner.assertValid(); // MAX_DATA_RATE is ignored
        runner.removeProperty(ControlRate.MAX_RATE);
        runner.assertNotValid(); // MAX_RATE is a required property for this rate control criteria

        runner.clearProperties();
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.FLOWFILE_RATE);
        runner.assertNotValid(); // MAX_RATE is not set
        runner.setProperty(ControlRate.MAX_RATE, "1 MB");
        runner.assertNotValid(); // MAX_RATE is not an integer
        runner.setProperty(ControlRate.MAX_RATE, "1");
        runner.assertValid();
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "1");
        runner.assertValid(); // MAX_COUNT_RATE is ignored
        runner.removeProperty(ControlRate.MAX_RATE);
        runner.assertNotValid(); // MAX_RATE is a required property for this rate control criteria

        runner.clearProperties();
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.ATTRIBUTE_RATE);
        runner.setProperty(ControlRate.RATE_CONTROL_ATTRIBUTE_NAME, "count");
        runner.assertNotValid(); // MAX_RATE is not set
        runner.setProperty(ControlRate.MAX_RATE, "1 MB");
        runner.assertNotValid(); // MAX_RATE is not an integer
        runner.setProperty(ControlRate.MAX_RATE, "1");
        runner.assertValid();
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "1");
        runner.assertValid(); // MAX_COUNT_RATE is ignored
        runner.removeProperty(ControlRate.MAX_RATE);
        runner.assertNotValid();// MAX_RATE is a required property for this rate control criteria
        runner.setProperty(ControlRate.MAX_RATE, "1");
        runner.removeProperty(ControlRate.RATE_CONTROL_ATTRIBUTE_NAME);
        runner.assertNotValid();// RATE_CONTROL_ATTRIBUTE_NAME is a required property for this rate control criteria

        runner.clearProperties();
        runner.setProperty(ControlRate.RATE_CONTROL_CRITERIA, ControlRate.DATA_OR_FLOWFILE_RATE);
        runner.setProperty(ControlRate.MAX_DATA_RATE, "1 MB");
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "1");
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "2");
        runner.assertValid(); // both MAX_DATA_RATE and MAX_COUNT_RATE are set
        runner.removeProperty(ControlRate.MAX_COUNT_RATE);
        runner.assertNotValid(); // MAX_COUNT_RATE is not set
        runner.setProperty(ControlRate.MAX_COUNT_RATE, "1");
        runner.removeProperty(ControlRate.MAX_DATA_RATE);
        runner.assertNotValid();// MAX_DATA_RATE is not set
        runner.setProperty(ControlRate.MAX_DATA_RATE, "1 MB");
        runner.setProperty(ControlRate.MAX_RATE, "1 MB");
        runner.assertValid(); // MAX_RATE is ignored
    }

    private void createFlowFile(final TestRunner runner, final int value) {
        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("count", String.valueOf(value));
        byte[] data = "0123456789".getBytes();
        runner.enqueue(data, attributeMap);
    }
    private void createFlowFileWithGroup(final TestRunner runner, final String group) {
        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("group", group);
        runner.enqueue(new byte[0], attributeMap);
    }

    private void incrementCurrentTime() {
        controlRate.currentTimeMillis += CURRENT_TIME_INCREMENT;
    }

    private void incrementCurrentTime(final long milliseconds) {
        controlRate.currentTimeMillis += milliseconds;
    }

    private static class ConfigurableControlRate extends ControlRate {

        private long currentTimeMillis = System.currentTimeMillis();

        @Override
        protected long getCurrentTimeMillis() {
            return currentTimeMillis;
        }
    }
}
