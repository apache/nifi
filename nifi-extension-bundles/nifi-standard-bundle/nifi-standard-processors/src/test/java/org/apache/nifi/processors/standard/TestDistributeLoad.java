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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDistributeLoad {

    @Test
    public void testDefaultRoundRobin() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());
        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS, "100");

        for (int i = 0; i < 101; i++) {
            testRunner.enqueue(new byte[0]);
        }

        testRunner.run(101);
        testRunner.assertTransferCount("1", 2);
        for (int i = 2; i <= 100; i++) {
            testRunner.assertTransferCount(String.valueOf(i), 1);
        }
    }

    @Test
    public void testWeightedRoundRobin() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());
        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS, "100");

        testRunner.setProperty("1", "5");
        testRunner.setProperty("2", "3");

        for (int i = 0; i < 106; i++) {
            testRunner.enqueue(new byte[0]);
        }

        testRunner.run(108);
        testRunner.assertTransferCount("1", 5);
        testRunner.assertTransferCount("2", 3);
        for (int i = 3; i <= 100; i++) {
            testRunner.assertTransferCount(String.valueOf(i), 1);
        }
    }

    @Test
    public void testValidationOnAddedProperties() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());
        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS, "100");

        testRunner.setProperty("1", "5");

        testRunner.setProperty("1", "0");
        testRunner.assertNotValid();

        testRunner.setProperty("1", "-1");
        testRunner.assertNotValid();

        testRunner.setProperty("1", "101");
        testRunner.setProperty("100", "5");

        testRunner.setProperty("101", "5");
        testRunner.assertNotValid();

        testRunner.setProperty("0", "5");
        testRunner.assertNotValid();

        testRunner.setProperty("-1", "5");
        testRunner.assertNotValid();
    }

    @Test
    public void testNextAvailable() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());

        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS.getName(), "100");
        testRunner.setProperty(DistributeLoad.DISTRIBUTION_STRATEGY.getName(), DistributeLoad.STRATEGY_NEXT_AVAILABLE.getValue());

        for (int i = 0; i < 99; i++) {
            testRunner.enqueue(new byte[0]);
        }

        testRunner.setRelationshipUnavailable("50");

        testRunner.run(101);
        testRunner.assertQueueEmpty();

        for (int i = 1; i <= 100; i++) {
            testRunner.assertTransferCount(String.valueOf(i), (i == 50) ? 0 : 1);
        }
    }

    @Test
    public void testFlowFileAttributesAdded() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());

        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS, "100");
        testRunner.setProperty(DistributeLoad.DISTRIBUTION_STRATEGY, DistributeLoad.STRATEGY_NEXT_AVAILABLE);

        for (int i = 0; i < 100; i++) {
            testRunner.enqueue(new byte[0]);
        }

        testRunner.run(101);
        testRunner.assertQueueEmpty();

        for (int i = 1; i <= 100; i++) {
            testRunner.assertTransferCount(String.valueOf(i), 1);
            final List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(String.valueOf(i));
            assertEquals(1, flowFilesForRelationship.size());
            final MockFlowFile mockFlowFile = flowFilesForRelationship.get(0);
            assertEquals(String.valueOf(i), mockFlowFile.getAttribute(DistributeLoad.RELATIONSHIP_ATTRIBUTE));
        }
    }

    @Test
    public void testOverflow() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());

        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS.getName(), "3");
        testRunner.setProperty(DistributeLoad.DISTRIBUTION_STRATEGY.getName(), DistributeLoad.STRATEGY_OVERFLOW.getValue());

        // Queue all FlowFiles required for this test
        for (int i = 0; i < 8; i++) {
            testRunner.enqueue(new byte[0]);
        }

        // Repeatedly send to highest weighted relationship as long as it is available
        testRunner.run(2, false);
        testRunner.assertTransferCount("1", 2);
        testRunner.assertTransferCount("2", 0);
        testRunner.assertTransferCount("3", 0);

        // When highest weighted relationship becomes unavailable, repeatedly send to next-highest weighted relationship
        testRunner.clearTransferState();
        testRunner.setRelationshipUnavailable("1");
        testRunner.run(2, false);
        testRunner.assertTransferCount("1", 0);
        testRunner.assertTransferCount("2", 2);
        testRunner.assertTransferCount("3", 0);

        // Return to highest weighted relationship when it becomes available again
        testRunner.clearTransferState();
        testRunner.setRelationshipAvailable("1");
        testRunner.run(2, false);
        testRunner.assertTransferCount("1", 2);
        testRunner.assertTransferCount("2", 0);
        testRunner.assertTransferCount("3", 0);

        // Skip ahead and repeatedly send to the first available relationship when multiple relationships are unavailable
        testRunner.clearTransferState();
        testRunner.setRelationshipUnavailable("1");
        testRunner.setRelationshipUnavailable("2");
        testRunner.run(2, false);
        testRunner.assertTransferCount("1", 0);
        testRunner.assertTransferCount("2", 0);
        testRunner.assertTransferCount("3", 2);
    }
}
