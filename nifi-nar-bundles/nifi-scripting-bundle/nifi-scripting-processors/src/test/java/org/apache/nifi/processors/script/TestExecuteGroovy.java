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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


public class TestExecuteGroovy {

    private TestRunner runner;

    /**
     * Copies all scripts to the target directory because when they are compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));
    }

    @Before
    public void setup() throws Exception {
        final ExecuteScript executeScript = new ExecuteScript();
        // Need to do something to initialize the properties, like retrieve the list of properties
        assertNotNull(executeScript.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(executeScript);
    }

    /**
     * Tests a script file that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_FILE, "target/test/resources/groovy/test_onTrigger.groovy");
        runner.setProperty(ExecuteScript.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBody() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY, "session.putAttribute(flowFile, \"from-content\", \"test content\")");
        runner.setProperty(ExecuteScript.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that has provides the body of an onTrigger() function, where the ExecuteScript processor does
     * not specify a modules path
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBodyNoModules() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY, "session.putAttribute(flowFile, \"from-content\", \"test content\")");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that uses a dynamic property to set a FlowFile attribute.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileCustomAttribute() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY, "session.putAttribute(flowFile, \"from-content\", \"${testprop}\")");
        runner.setProperty("testprop", "test content");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that throws an Exception within. The expected result is that the FlowFile will be routed to
     * failure
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptException() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY, "throw new Exception()");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_FAILURE, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_FAILURE);
        assertFalse(result.isEmpty());
    }

    /**
     * Tests a script that does not return a FlowFile. The expected result is that the FlowFile will be routed to
     * no-flowfile
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptDoesNotReturnFlowFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY, "'This is not a FlowFile'");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_NOFLOWFILE, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_NOFLOWFILE);
        assertFalse(result.isEmpty());
    }

}
