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
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecuteScriptGroovyTest extends BaseScriptTest {
    private static final Pattern SINGLE_POOL_THREAD_PATTERN = Pattern.compile("pool-\\d+-thread-1");

    @BeforeEach
    public void setUp() throws Exception {
        super.setupExecuteScript();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy");
    }

    @Test
    void testShouldExecuteScript() {
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS).getFirst();
        flowFile.assertAttributeExists("time-updated");
        flowFile.assertAttributeExists("thread");
        assertTrue(SINGLE_POOL_THREAD_PATTERN.matcher(flowFile.getAttribute("thread")).find());
    }

    @Test
    void testShouldExecuteScriptSerially() {
        final int iterations = 10;
        runner.assertValid();

        runner.run(iterations);

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, iterations);
        runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS).forEach( flowFile -> {
            flowFile.assertAttributeExists("time-updated");
            flowFile.assertAttributeExists("thread");
            assertTrue(SINGLE_POOL_THREAD_PATTERN.matcher(flowFile.getAttribute("thread")).find());
        });
    }

    @Test
    void testShouldExecuteScriptWithPool() {
        final int iterations = 10;
        final int poolSize = 2;

        setupPooledExecuteScript(poolSize);
        runner.setThreadCount(poolSize);
        runner.assertValid();

        runner.run(iterations);

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, iterations);
        runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS).forEach(flowFile -> {
            flowFile.assertAttributeExists("time-updated");
            flowFile.assertAttributeExists("thread");
            assertTrue((Pattern.compile("pool-\\d+-thread-[1-" + poolSize + "]").matcher(flowFile.getAttribute("thread"))).find());
        });
    }

    @Test
    void testExecuteScriptRecompileOnChange() {

        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/setAttributeHello_executescript.groovy");
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS).getFirst();
        flowFile.assertAttributeExists("greeting");
        flowFile.assertAttributeEquals("greeting", "hello");
        runner.clearTransferState();

        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/setAttributeGoodbye_executescript.groovy");
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS).getFirst();
        flowFile.assertAttributeExists("greeting");
        flowFile.assertAttributeEquals("greeting", "good-bye");
    }

    private void setupPooledExecuteScript(int poolSize) {
        final ExecuteScript executeScript = new ExecuteScript();
        // Need to do something to initialize the properties, like retrieve the list of properties
        assertNotNull(executeScript.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(executeScript);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "groovy/testAddTimeAndThreadAttribute.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy");

        // Override userContext value
        ((MockProcessContext) runner.getProcessContext()).setMaxConcurrentTasks(poolSize);
    }
}
