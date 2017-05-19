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


public class TestExecuteClojure extends BaseScriptTest {

    public final String TEST_CSV_DATA = "gender,title,first,last\n"
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
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Clojure");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "clojure/test_onTrigger.clj");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/clojure");

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
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Clojure");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "clojure/test_onTrigger.clj");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/clojure");

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
    public void testCreateNewFlowFileWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Clojure");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "clojure/test_onTrigger_newFlowFile.clj");
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "clojure");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        // The script removes the original file and transfers only the new one
        assertEquals(1, runner.getRemovedCount());
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("selected.columns", "title,first");
        result.get(0).assertAttributeEquals("filename", "split_cols.txt");
    }

    /**
     * Tests a script file that uses dynamic properties defined on the processor.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testDynamicProperties() throws Exception {
        runner.setValidateExpressionUsage(true);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Clojure");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "clojure/test_dynamicProperties.clj");
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
}
