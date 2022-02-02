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

package org.apache.nifi.processors.stateless;

import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestExecuteStateless {
    private static final String HELLO_WORLD = "Hello World";
    private static final String LIB_DIR = "target/nifi-stateless-processors-test-assembly";
    private static final String WORK_DIR = "target/work";

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(ExecuteStateless.class);
        runner.setProperty(ExecuteStateless.DATAFLOW_SPECIFICATION_STRATEGY, ExecuteStateless.SPEC_FROM_FILE);
        runner.setProperty(ExecuteStateless.LIB_DIRECTORY, LIB_DIR);
        runner.setProperty(ExecuteStateless.WORKING_DIRECTORY, WORK_DIR);
    }

    @Test
    public void testSimplePassThrough() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/passthrough-flow.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");

        runner.enqueue(HELLO_WORLD.getBytes(), Collections.singletonMap("abc", "xyz"));
        runner.run();

        runner.assertTransferCount(ExecuteStateless.REL_OUTPUT, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ExecuteStateless.REL_OUTPUT).get(0);
        output.assertAttributeEquals("abc", "xyz");
        output.assertContentEquals(HELLO_WORLD);
    }

    @Test
    public void testSplitWithParameters() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/split-text.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");
        runner.setProperty("Lines Per Split", "3");

        runner.enqueue("The\nQuick\nBrown\nFox\nJumps\nOver\nThe\nLazy\nDog".getBytes(), Collections.singletonMap("abc", "xyz"));
        runner.run();

        runner.assertTransferCount(ExecuteStateless.REL_OUTPUT, 3);
        final List<MockFlowFile> output = runner.getFlowFilesForRelationship(ExecuteStateless.REL_OUTPUT);
        output.forEach(ff -> ff.assertAttributeEquals("abc", "xyz"));
        output.get(0).assertContentEquals("The\nQuick\nBrown");
        output.get(1).assertContentEquals("Fox\nJumps\nOver");
        output.get(2).assertContentEquals("The\nLazy\nDog");
    }

    @Test
    public void testRouteToFailure() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/route-one-to-failure.json");
        runner.setProperty(ExecuteStateless.FAILURE_PORTS, "Last");

        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteStateless.REL_OUTPUT, 0);
    }


    @Test
    public void testRouteToFailureWithInput() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/route-to-desired-port.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");
        runner.setProperty(ExecuteStateless.FAILURE_PORTS, "Other");

        runner.enqueue("A", Collections.singletonMap("desired.port", "A"));
        runner.enqueue("B", Collections.singletonMap("desired.port", "B"));
        runner.enqueue("C", Collections.singletonMap("desired.port", "C"));

        runner.run(3, true, true, 60000L);

        runner.assertTransferCount(ExecuteStateless.REL_OUTPUT, 3);
        runner.assertTransferCount(ExecuteStateless.REL_ORIGINAL, 3);
        runner.assertTransferCount(ExecuteStateless.REL_FAILURE, 0);
        runner.assertTransferCount(ExecuteStateless.REL_TIMEOUT, 0);

        runner.clearTransferState();
        runner.enqueue("D", Collections.singletonMap("desired.port", "D"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteStateless.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(ExecuteStateless.REL_FAILURE).get(0).assertAttributeEquals("failure.port.name", "Other");
    }


    @Test
    public void testMultipleFailurePortNames() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/route-to-desired-port.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");
        runner.setProperty(ExecuteStateless.FAILURE_PORTS, "Other, A,    B,C");

        runner.enqueue("B", Collections.singletonMap("desired.port", "B"));

        runner.run();

        runner.assertTransferCount(ExecuteStateless.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(ExecuteStateless.REL_FAILURE).get(0).assertAttributeEquals("failure.port.name", "B");
    }

    @Test
    public void testRouteToFailureInnerGroup() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/route-to-failure-inner-group.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");
        runner.setProperty(ExecuteStateless.FAILURE_PORTS, "failure");

        runner.enqueue("Hello World");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteStateless.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(ExecuteStateless.REL_FAILURE).get(0).assertAttributeEquals("failure.port.name", "failure");
    }

    @Test
    public void testTimeout() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/sleep.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");
        runner.setProperty(ExecuteStateless.DATAFLOW_TIMEOUT, "100 millis");
        runner.setProperty("Duration", "5 sec"); // Have DebugFlow sleep for 5 seconds


        runner.enqueue("Hello World");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteStateless.REL_TIMEOUT, 1);
    }

    @Test
    public void testProcessorExceptionRoutesToFailure() {
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/throw-exception.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");

        runner.enqueue("Hello World");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteStateless.REL_FAILURE, 1);
    }

    @Test
    public void testInfoBulletinNotSurfaced() {
        testBulletinSurfaced("INFO", false, MockComponentLog::getInfoMessages);
    }

    @Test
    public void testWarnBulletinSurfaced() {
        testBulletinSurfaced("WARN", true, MockComponentLog::getWarnMessages);
    }

    @Test
    public void testErrorBulletinSurfaced() {
        testBulletinSurfaced("ERROR", true, MockComponentLog::getErrorMessages);
    }

    private void testBulletinSurfaced(final String logLevel, final boolean shouldBeSurfaced, final Function<MockComponentLog, List<LogMessage>> getMessageFunction) {
        final String logMessage = "Unit Test Message";

        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/log-message.json");
        runner.setProperty(ExecuteStateless.INPUT_PORT, "In");
        runner.setProperty("Log Message", logMessage);
        runner.setProperty("Log Level", logLevel);

        runner.enqueue("Hello World");
        runner.run();

        runner.assertTransferCount(ExecuteStateless.REL_ORIGINAL, 1);
        final List<LogMessage> logMessages = getMessageFunction.apply(runner.getLogger());
        final long matchingMessageCount = logMessages.stream()
            .filter(msg -> msg.getMsg().contains(logMessage))
            .count();

        if (shouldBeSurfaced) {
            assertTrue(matchingMessageCount > 0);
        } else {
            assertEquals(0, matchingMessageCount);
        }
    }
}
