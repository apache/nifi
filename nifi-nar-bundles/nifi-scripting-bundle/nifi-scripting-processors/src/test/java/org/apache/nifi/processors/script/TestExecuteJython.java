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
package org.apache.nifi.processors.script;

import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for ExecuteScript with Jython.
 */
public class TestExecuteJython extends BaseScriptTest {

    @BeforeEach
    public void setup() throws Exception {
        super.setupExecuteScript();
    }

    /**
     * Tests a Jython script that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBody() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY,
                "from org.apache.nifi.processors.script import ExecuteScript\n"
                        + "flowFile = session.get()\n"
                        + "flowFile = session.putAttribute(flowFile, \"from-content\", \"test content\")\n"
                        + "session.transfer(flowFile, ExecuteScript.REL_SUCCESS)");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a Jython script that references an outside python module
     *
     */
    @Test
    public void testAccessModuleAndStoreInFlowFileAttributeWithScriptBody() {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/jython/");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY,
                "from org.apache.nifi.processors.script import ExecuteScript\n"
                        + "from test_external_module import ExternalModule\n"
                        + "externalModule = ExternalModule()\n"
                        + "flowFile = session.get()\n"
                        + "flowFile = session.putAttribute(flowFile, \"key\", externalModule.testHelloWorld())\n"
                        + "session.transfer(flowFile, ExecuteScript.REL_SUCCESS)");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("key", "helloWorld");
    }

    /**
     * Tests a script that does not transfer or remove the original flow file, thereby causing an error during commit.
     *
     */
    @Test
    public void testScriptNoTransfer() {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY,
                "flowFile = session.putAttribute(flowFile, \"from-content\", \"test content\")\n");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        assertThrows(AssertionError.class, () -> runner.run());
    }

    @EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true")
    @Test
    public void testPerformance() {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY,
                "from org.apache.nifi.processors.script import ExecuteScript\n"
                        + "flowFile = session.get()\n"
                        + "flowFile = session.putAttribute(flowFile, \"from-content\", \"test content\")\n"
                        + "session.transfer(flowFile, ExecuteScript.REL_SUCCESS)");

        runner.assertValid();
        final int ITERATIONS = 50000;
        for (int i = 0; i < ITERATIONS; i++) {
            runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        }
        runner.run(ITERATIONS);

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, ITERATIONS);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }
}
