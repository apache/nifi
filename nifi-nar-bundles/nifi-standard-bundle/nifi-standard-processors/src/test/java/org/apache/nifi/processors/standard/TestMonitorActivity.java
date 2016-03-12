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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class TestMonitorActivity {

    @Test
    public void testFirstMessage() throws InterruptedException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(1000L));
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        runner.clearTransferState();

        Thread.sleep(1000L);

        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        // ensure we don't keep creating the message
        for (int i = 0; i < 10; i++) {
            runner.run();
            runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
            runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
            runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
            Thread.sleep(100L);
        }

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        String flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");
        Thread.sleep(200L);

        for (int i = 0; i < 10; i++) {
            runner.run();
            Thread.sleep(200L);
        }

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 10);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.clearTransferState();

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);

        restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeNotExists("key");
        restoredFlowFile.assertAttributeNotExists("key1");
    }

    @Test
    public void testFirstMessageWithInherit() throws InterruptedException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestableProcessor(1000L));
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        runner.setProperty(MonitorActivity.THRESHOLD, "100 millis");
        runner.setProperty(MonitorActivity.COPY_ATTRIBUTES, "true");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_SUCCESS, 1);
        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_SUCCESS).get(0);
        runner.clearTransferState();

        Thread.sleep(1000L);

        runner.run();
        runner.assertAllFlowFilesTransferred(MonitorActivity.REL_INACTIVE, 1);
        runner.clearTransferState();

        // ensure we don't keep creating the message
        for (int i = 0; i < 10; i++) {
            runner.run();
            runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
            runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
            runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
            Thread.sleep(100L);
        }

        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        attributes.put("key1", "value1");

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);

        MockFlowFile restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        String flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeEquals("key", "value");
        restoredFlowFile.assertAttributeEquals("key1", "value1");

        // verify the UUIDs are not the same
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.UUID.key(), originalFlowFile.getAttribute(CoreAttributes.UUID.key()));
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.FILENAME.key(), originalFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertTrue(
                String.format("file sizes match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getSize(), restoredFlowFile.getSize()), restoredFlowFile.getSize() != originalFlowFile.getSize());
        Assert.assertTrue(
                String.format("lineage start dates match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getLineageStartDate(), restoredFlowFile.getLineageStartDate()), restoredFlowFile.getLineageStartDate() != originalFlowFile.getLineageStartDate());

        runner.clearTransferState();
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "true");
        Thread.sleep(200L);

        for (int i = 0; i < 10; i++) {
            runner.run();
            Thread.sleep(200L);
        }

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 10);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
        runner.clearTransferState();

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
        runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 1);
        runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 1);

        restoredFlowFile = runner.getFlowFilesForRelationship(MonitorActivity.REL_ACTIVITY_RESTORED).get(0);
        flowFileContent = new String(restoredFlowFile.toByteArray());
        Assert.assertTrue(Pattern.matches("Activity restored at time: (.*) after being inactive for 0 minutes", flowFileContent));
        restoredFlowFile.assertAttributeEquals("key", "value");
        restoredFlowFile.assertAttributeEquals("key1", "value1");
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.UUID.key(), originalFlowFile.getAttribute(CoreAttributes.UUID.key()));
        restoredFlowFile.assertAttributeNotEquals(CoreAttributes.FILENAME.key(), originalFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        Assert.assertTrue(
                String.format("file sizes match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getSize(), restoredFlowFile.getSize()), restoredFlowFile.getSize() != originalFlowFile.getSize());
        Assert.assertTrue(
                String.format("lineage start dates match when they shouldn't original=%1$s restored=%2$s",
                        originalFlowFile.getLineageStartDate(), restoredFlowFile.getLineageStartDate()), restoredFlowFile.getLineageStartDate() != originalFlowFile.getLineageStartDate());
    }

    @Test(timeout=5000)
    public void testFirstRunNoMessages() throws InterruptedException, IOException {
        // don't use the TestableProcessor, we want the real timestamp from @OnScheduled
        final TestRunner runner = TestRunners.newTestRunner(new MonitorActivity());
        runner.setProperty(MonitorActivity.CONTINUALLY_SEND_MESSAGES, "false");
        int threshold = 100;
        boolean rerun = false;
        do {
            rerun = false;
            runner.setProperty(MonitorActivity.THRESHOLD, threshold + " millis");

            Thread.sleep(1000L);

            // shouldn't generate inactivity b/c run() will reset the lastSuccessfulTransfer if @OnSchedule & onTrigger
            // does not  get called more than MonitorActivity.THRESHOLD apart
            runner.run();
            runner.assertTransferCount(MonitorActivity.REL_SUCCESS, 0);
            List<MockFlowFile> inactiveFlowFiles = runner.getFlowFilesForRelationship(MonitorActivity.REL_INACTIVE);
            if (inactiveFlowFiles.size() == 1) {
                // Seems Threshold was not sufficient, which has caused One inactive message.
                // Step-up and rerun the test until successful or jUnit Timesout
                threshold += threshold;
                rerun = true;
            } else {
                runner.assertTransferCount(MonitorActivity.REL_INACTIVE, 0);
            }
            runner.assertTransferCount(MonitorActivity.REL_ACTIVITY_RESTORED, 0);
            runner.clearTransferState();
        } while(rerun);
    }

    /**
     * Since each call to run() will call @OnScheduled methods which will set the lastSuccessfulTransfer to the
     * current time, we need a way to create an artificial time difference between calls to run.
     */
    private class TestableProcessor extends MonitorActivity {

        private final long timestampDifference;

        public TestableProcessor(final long timestampDifference) {
            this.timestampDifference = timestampDifference;
        }

        @Override
        public void resetLastSuccessfulTransfer() {
            setLastSuccessfulTransfer(System.currentTimeMillis() - timestampDifference);
        }
    }
}
