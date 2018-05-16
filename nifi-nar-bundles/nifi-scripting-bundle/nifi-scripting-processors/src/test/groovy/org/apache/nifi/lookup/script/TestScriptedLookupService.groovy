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

import org.apache.commons.io.FileUtils
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.controller.ControllerServiceInitializationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.MockPropertyValue
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static junit.framework.TestCase.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

/**
 * Unit tests for the ScriptedLookupService controller service
 */
class TestScriptedLookupService {

    private static final Logger logger = LoggerFactory.getLogger(TestScriptedLookupService)
    ScriptedLookupService scriptedLookupService
    def scriptingComponent


    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = {String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
        FileUtils.copyDirectory('src/test/resources' as File, 'target/test/resources' as File)
    }

    @Before
    void setUp() {
        scriptedLookupService = new MockScriptedLookupService()
        scriptingComponent = (AccessibleScriptingComponentHelper) scriptedLookupService
    }

    @Test
    void testLookupServiceGroovyScript() {

        def properties = [:] as Map<PropertyDescriptor, String>
        scriptedLookupService.getSupportedPropertyDescriptors().each {PropertyDescriptor descriptor ->
            properties.put(descriptor, descriptor.getDefaultValue())
        }

        // Mock the ConfigurationContext for setup(...)
        def configurationContext = mock(ConfigurationContext)
        when(configurationContext.getProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue('Groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue('target/test/resources/groovy/test_lookup_inline.groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null))

        def logger = mock(ComponentLog)
        def initContext = mock(ControllerServiceInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        scriptedLookupService.initialize initContext
        scriptedLookupService.onEnabled configurationContext

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
