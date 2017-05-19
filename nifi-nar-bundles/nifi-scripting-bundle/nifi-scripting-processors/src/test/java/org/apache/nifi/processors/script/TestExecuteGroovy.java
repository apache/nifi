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
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TestExecuteGroovy extends BaseScriptTest {

    private final String TEST_CSV_DATA = "gender,title,first,last\n"
            + "female,miss,marlene,shaw\n"
            + "male,mr,todd,graham";

    @Before
    public void setup() throws Exception {
        super.setupExecuteScript();
    }

    /**
     * Tests a script file that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/test_onTrigger.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script file that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testNoIncomingFlowFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/test_onTrigger.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.run();

        runner.assertTransferCount(ExecuteScript.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteScript.REL_FAILURE, 0);
    }

    /**
     * Tests a script file that creates and transfers a new flow file.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testInvalidConfiguration() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION);
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "body");

        runner.assertNotValid();
    }

    /**
     * Tests a script file that creates and transfers a new flow file.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testCreateNewFlowFileWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/test_onTrigger_newFlowFile.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        // The script removes the original file and transfers only the new one
        assertEquals(1, runner.getRemovedCount());
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("filename", "split_cols.txt");
    }

    /**
     * Tests a script file that creates and transfers a new flow file.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testCreateNewFlowFileWithNoInputFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY,
                getFileContentsAsString(TEST_RESOURCE_LOCATION + "groovy/testCreateNewFlowFileWithNoInputFile.groovy")
        );

        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("filename", "newfile");
    }

    /**
     * Tests a script file that creates and transfers a new flow file.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testDynamicProperties() throws Exception {
        runner.setValidateExpressionUsage(true);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/test_dynamicProperties.groovy");
        runner.setProperty("myProp", "${myAttr}");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8),
                new HashMap<String, String>(1) {{
                    put("myAttr", "testValue");
                }});
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "testValue");
    }

    /**
     * Tests a script file that changes the content of the incoming flowfile.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testChangeFlowFileWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_onTrigger_changeContent.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        MockFlowFile resultFile = result.get(0);
        resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Marlene Shaw\nTodd Graham\n");
    }


    /**
     * Tests a script that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBody() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "groovy/testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBody.groovy")
        );
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy");

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
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "groovy/testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBodyNoModules.groovy")
        );
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that does not transfer or remove the original flow file, thereby causing an error during commit.
     *
     * @throws Exception Any error encountered while testing. Expecting
     */
    @Test(expected = AssertionError.class)
    public void testScriptNoTransfer() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "groovy/testScriptNoTransfer.groovy")
        );

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

    }

    /**
     * Tests a script that uses a dynamic property to set a FlowFile attribute.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileCustomAttribute() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "groovy/testReadFlowFileContentAndStoreInFlowFileCustomAttribute.groovy")
        );
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
    @Test(expected = AssertionError.class)
    public void testScriptException() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "throw new Exception()");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();
    }
}
