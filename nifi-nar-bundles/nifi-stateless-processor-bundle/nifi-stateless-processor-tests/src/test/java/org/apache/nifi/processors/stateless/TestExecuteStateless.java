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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestExecuteStateless {
    private static final String HELLO_WORLD = "Hello World";
    private static final String LIB_DIR = "target/nifi-stateless-processors-test-assembly";
    private static final String WORK_DIR = "target/work";

    @Test
    public void testSimplePassThrough() {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteStateless.class);
        runner.setProperty(ExecuteStateless.DATAFLOW_SPECIFICATION_STRATEGY, ExecuteStateless.SPEC_FROM_FILE);
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/passthrough-flow.json");
        runner.setProperty(ExecuteStateless.LIB_DIRECTORY, LIB_DIR);
        runner.setProperty(ExecuteStateless.WORKING_DIRECTORY, WORK_DIR);
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
        final TestRunner runner = TestRunners.newTestRunner(ExecuteStateless.class);
        runner.setProperty(ExecuteStateless.DATAFLOW_SPECIFICATION_STRATEGY, ExecuteStateless.SPEC_FROM_FILE);
        runner.setProperty(ExecuteStateless.DATAFLOW_FILE, "src/test/resources/split-text.json");
        runner.setProperty(ExecuteStateless.LIB_DIRECTORY, LIB_DIR);
        runner.setProperty(ExecuteStateless.WORKING_DIRECTORY, WORK_DIR);
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

}
