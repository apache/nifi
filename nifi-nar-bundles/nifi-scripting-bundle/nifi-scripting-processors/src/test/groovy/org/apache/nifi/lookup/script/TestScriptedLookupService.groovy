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
package org.apache.nifi.lookup.script

import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import static junit.framework.TestCase.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
/**
 * Unit tests for the ScriptedLookupService controller service
 */
class TestScriptedLookupService {
    private static final String GROOVY_SCRIPT = "test_lookup_inline.groovy"
    private static final Path SOURCE_PATH = Paths.get("src/test/resources/groovy", GROOVY_SCRIPT)
    private static final Path TARGET_PATH = Paths.get("target", GROOVY_SCRIPT)
    private static final Logger logger = LoggerFactory.getLogger(TestScriptedLookupService)
    ScriptedLookupService scriptedLookupService
    def scriptingComponent


    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = {String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
        Files.copy(SOURCE_PATH, TARGET_PATH, StandardCopyOption.REPLACE_EXISTING)
        TARGET_PATH.toFile().deleteOnExit()
    }

    @Before
    void setUp() {
        scriptedLookupService = new MockScriptedLookupService()
        scriptingComponent = (AccessibleScriptingComponentHelper) scriptedLookupService
    }

    @Test
    void testLookupServiceGroovyScript() {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        runner.addControllerService("lookupService", scriptedLookupService);
        runner.setProperty(scriptedLookupService, "Script Engine", "Groovy");
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.SCRIPT_FILE, TARGET_PATH.toString());
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(scriptedLookupService, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(scriptedLookupService);

        MockFlowFile mockFlowFile = new MockFlowFile(1L)
        InputStream inStream = new ByteArrayInputStream('Flow file content not used'.bytes)

        Optional opt = scriptedLookupService.lookup(['key':'Hello'])
        assertTrue(opt.present)
        assertEquals('Hi', opt.get())
        opt = scriptedLookupService.lookup(['key':'World'])
        assertTrue(opt.present)
        assertEquals('there', opt.get())
        opt = scriptedLookupService.lookup(['key':'Not There'])
        assertFalse(opt.present)
    }

    class MockScriptedLookupService extends ScriptedLookupService implements AccessibleScriptingComponentHelper {

        @Override
        ScriptingComponentHelper getScriptingComponentHelper() {
            return this.@scriptingComponentHelper
        }
    }
}
