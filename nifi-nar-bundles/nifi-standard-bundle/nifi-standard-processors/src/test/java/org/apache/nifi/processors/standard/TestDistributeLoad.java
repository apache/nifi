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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDistributeLoad {

    @BeforeClass
    public static void before() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.DistributeLoad", "debug");
    }

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

        try {
            testRunner.setProperty("1", "0");
            Assert.fail("Allows property '1' to be set to '0'");
        } catch (final AssertionError e) {
            // expected behavior
        }

        try {
            testRunner.setProperty("1", "-1");
            Assert.fail("Allows property '1' to be set to '-1'");
        } catch (final AssertionError e) {
            // expected behavior
        }

        testRunner.setProperty("1", "101");
        testRunner.setProperty("100", "5");

        try {
            testRunner.setProperty("101", "5");
            Assert.fail("Allows property '101' to be set to '5'");
        } catch (final AssertionError e) {
            // expected behavior
        }

        try {
            testRunner.setProperty("0", "5");
            Assert.fail("Allows property '0' to be set to '5'");
        } catch (final AssertionError e) {
            // expected behavior
        }

        try {
            testRunner.setProperty("-1", "5");
            Assert.fail("Allows property '-1' to be set to '5'");
        } catch (final AssertionError e) {
            // expected behavior
        }
    }

    @Test
    public void testNextAvailable() {
        final TestRunner testRunner = TestRunners.newTestRunner(new DistributeLoad());

        testRunner.setProperty(DistributeLoad.NUM_RELATIONSHIPS.getName(), "100");
        testRunner.setProperty(DistributeLoad.DISTRIBUTION_STRATEGY.getName(), DistributeLoad.STRATEGY_NEXT_AVAILABLE);

        for (int i = 0; i < 99; i++) {
            testRunner.enqueue(new byte[0]);
        }

        testRunner.setRelationshipUnavailable("50");

        testRunner.run(101);
        testRunner.assertQueueEmpty();

        for (int i = 1; i <= 100; i++) {
            System.out.println(i);
            testRunner.assertTransferCount(String.valueOf(i), (i == 50) ? 0 : 1);
        }
    }
}
