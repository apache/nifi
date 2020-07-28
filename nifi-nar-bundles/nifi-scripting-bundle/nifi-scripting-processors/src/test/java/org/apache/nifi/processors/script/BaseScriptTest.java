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
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;

/**
 * An abstract class with common methods, variables, etc. used by scripting processor unit tests
 */
public abstract class BaseScriptTest {

    public final String TEST_RESOURCE_LOCATION = "target/test/resources/";

    protected TestRunner runner;
    protected AccessibleScriptingComponentHelper scriptingComponent;

    /**
     * Copies all scripts to the target directory because when they are compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));
    }

    public void setupExecuteScript() throws Exception {
        final ExecuteScript executeScript = new AccessibleExecuteScript();
        // Need to do something to initialize the properties, like retrieve the list of properties
        assertNotNull(executeScript.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(executeScript);
        scriptingComponent = (AccessibleScriptingComponentHelper) executeScript;
    }

    public void setupInvokeScriptProcessor() throws Exception {
        final InvokeScriptedProcessor invokeScriptedProcessor = new AccessibleInvokeScriptedProcessor();
        // Need to do something to initialize the properties, like retrieve the list of properties
        assertNotNull(invokeScriptedProcessor.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(invokeScriptedProcessor);
        scriptingComponent = (AccessibleScriptingComponentHelper) invokeScriptedProcessor;
    }

    public String getFileContentsAsString(String path) {
        try {
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException ioe) {
            return null;
        }
    }

    class AccessibleExecuteScript extends ExecuteScript implements AccessibleScriptingComponentHelper {
        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }

    class AccessibleInvokeScriptedProcessor extends InvokeScriptedProcessor implements AccessibleScriptingComponentHelper {
        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}
