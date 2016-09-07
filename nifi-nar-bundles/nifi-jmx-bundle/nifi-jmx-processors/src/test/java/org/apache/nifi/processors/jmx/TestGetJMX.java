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

package org.apache.nifi.processors.jmx;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestGetJMX {
    @Test
    public void validatePropertiesWithRequiredValues() throws Exception {
        GetJMX processor = new GetJMX();

        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(processor.HOSTNAME, "hostname");
        runner.setProperty(processor.PORT, "9999");
        runner.setProperty(processor.BATCH_SIZE, "100");
        runner.setProperty(processor.POLLING_INTERVAL, "5 min");

        runner.removeProperty(processor.HOSTNAME);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("is invalid because Hostname is required"));
        }

        runner.setProperty(processor.HOSTNAME, "localhost");
        runner.removeProperty(processor.PORT);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("is invalid because Port is required"));
        }
    }

    @Test
    public void validatePropertiesValueTypes() {
        GetJMX processor = new GetJMX();

        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(processor.HOSTNAME, "localhost");
        runner.setProperty(processor.PORT, "abc");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("not a valid integer"));
        }

        runner.setProperty(processor.BATCH_SIZE, "abc");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("not a valid integer"));
        }

        runner.setProperty(processor.POLLING_INTERVAL, "abc");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("non-negative integer and TimeUnit"));
        }
    }
}
