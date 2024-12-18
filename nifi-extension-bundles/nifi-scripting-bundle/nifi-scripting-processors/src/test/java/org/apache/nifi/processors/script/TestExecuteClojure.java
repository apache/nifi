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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestExecuteClojure extends BaseScriptTest {

    public final String TEST_CSV_DATA = """
            gender,title,first,last
            female,miss,marlene,shaw
            male,mr,todd,graham""";

    @BeforeEach
    public void setup() throws Exception {
        super.setupExecuteScript();
    }

    /**
     * Tests a script file that has provides the body of an onTrigger() function.
     *
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptFile() {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Clojure");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "clojure/test_onTrigger.clj");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/clojure");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.getFirst().assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script file that has provides the body of an onTrigger() function.
     *
     */
    @Test
    public void testNoIncomingFlowFile() {
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
     */
    @Test
    public void testCreateNewFlowFileWithScriptFile() {
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
        result.getFirst().assertAttributeEquals("selected.columns", "title,first");
        result.getFirst().assertAttributeEquals("filename", "split_cols.txt");
    }

    /**
     * Tests a script file that uses dynamic properties defined on the processor.
     *
     */
    @Test
    public void testDynamicProperties() {
        runner.setValidateExpressionUsage(true);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Clojure");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "clojure/test_dynamicProperties.clj");
        runner.setProperty("myProp", "${myAttr}");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8), Map.of("myAttr", "testValue"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.getFirst().assertAttributeEquals("from-content", "testValue");
    }
}
