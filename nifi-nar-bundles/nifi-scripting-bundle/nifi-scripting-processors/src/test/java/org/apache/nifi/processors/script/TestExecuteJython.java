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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
     * Tests a Jython script when adding comma separated list of all existing directories from PYTHONPATH
     * as value for {@link ScriptingComponentUtils} MODULES property.
     *
     * @throws Exception If PYTHONPATH cannot be retrieved.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    public void testAccessPythonPackageModulesAndStoreInFlowFileAttributeWithScriptBody() throws Exception {
        String attributeName = "key";
        String attributeValue = "helloWorld";
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.MODULES,
                String.join(",", getExistingPythonPathModuleDirectories()));
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY,
                "from org.apache.nifi.processors.script import ExecuteScript\n"
                        + "flowFile = session.get()\n"
                        + "flowFile = session.putAttribute(flowFile," +  "\"" + attributeName + "\", '" + attributeValue + "')\n"
                        + "session.transfer(flowFile, ExecuteScript.REL_SUCCESS)");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals(attributeName, attributeValue);
    }

    /**
     * Method which retrieves the PYTHONPATH and builds a java.util.List of existing directories on the PYTHONPATH.
     * This method uses java.lang.ProcessBuilder and java.lang.Process to execute python to obtain the PYTHONPATH.
     *
     * @return java.util.List of existing directories on PYTHONPATH.
     * @throws Exception If an error occurs when executing a java.lang.Process.
     */
    List<String> getExistingPythonPathModuleDirectories() throws Exception{
        String pathDelimiter = ",";
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("python", "-c", "import sys; print('" + pathDelimiter + "'.join(sys.path))");
        Process process = processBuilder.start();
        String pythonPath = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);

        return Arrays.stream(pythonPath.split(pathDelimiter))
                .filter(path -> new File(path).isDirectory())
                .collect(Collectors.toList());
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
