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
package org.apache.nifi.reporting.script

import org.apache.commons.io.FileUtils
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.provenance.ProvenanceEventRecord
import org.apache.nifi.registry.VariableRegistry
import org.apache.nifi.reporting.ReportingInitializationContext
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.util.MockConfigurationContext
import org.apache.nifi.util.MockEventAccess
import org.apache.nifi.util.MockReportingContext
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.mockito.Mockito.*
/**
 * Unit tests for ScriptedReportingTask.
 */
@RunWith(JUnit4.class)
class ScriptedReportingTaskTest {
    private static final Logger logger = LoggerFactory.getLogger(ScriptedReportingTaskTest)
    def task
    def runner
    def scriptingComponent


    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
        FileUtils.copyDirectory('src/test/resources' as File, 'target/test/resources' as File)
    }

    @Before
    void setUp() {
        task = new MockScriptedReportingTask()
        runner = TestRunners
        scriptingComponent = (AccessibleScriptingComponentHelper) task
    }

    @Test
    void testProvenanceGroovyScript() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(new PropertyDescriptor.Builder().name("Script Engine").build(), "Groovy");
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, 'target/test/resources/groovy/test_log_provenance_events.groovy');

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null)

        final MockReportingContext context = new MockReportingContext([:], null, VariableRegistry.EMPTY_REGISTRY)
        context.setProperty("Script Engine", "Groovy")
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE.name, 'target/test/resources/groovy/test_log_provenance_events.groovy');

        final MockEventAccess eventAccess = context.getEventAccess();
        4.times { i ->
            eventAccess.addProvenanceEvent(createProvenanceEvent(i))
        }

        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.getSupportedPropertyDescriptors()

        task.setup configurationContext
        task.onTrigger context

        // This script should return a variable x with the number of events and a variable e with the first event
        def se = task.scriptEngine
        assertEquals 3, se.x
        assertEquals '1234', se.e.componentId
        assertEquals 'xyz', se.e.attributes.abc
        task.offerScriptEngine(se)
    }

    private ProvenanceEventRecord createProvenanceEvent(final long id) {
        final ProvenanceEventRecord event = mock(ProvenanceEventRecord.class)
        doReturn(id).when(event).getEventId()
        doReturn('1234').when(event).getComponentId()
        doReturn(['abc': 'xyz']).when(event).getAttributes()
        return event;
    }


    @Test
    void testVMEventsGroovyScript() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(new PropertyDescriptor.Builder().name("Script Engine").build(), "Groovy");
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, 'target/test/resources/groovy/test_log_vm_stats.groovy');

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null)

        final MockReportingContext context = new MockReportingContext([:], null, VariableRegistry.EMPTY_REGISTRY)
        context.setProperty("Script Engine", "Groovy")
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE.name, 'target/test/resources/groovy/test_log_vm_stats.groovy');

        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.getSupportedPropertyDescriptors()

        task.setup configurationContext
        task.onTrigger context
        def se = task.scriptEngine
        // This script should store a variable called x with a map of stats to values
        assertTrue se.x?.uptime >= 0
        task.offerScriptEngine(se)

    }

    @Test
    void testVMEventsJythonScript() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(new PropertyDescriptor.Builder().name("Script Engine").build(), "Groovy");
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, 'target/test/resources/groovy/test_log_vm_stats.groovy');

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null)

        final MockReportingContext context = new MockReportingContext([:], null, VariableRegistry.EMPTY_REGISTRY)
        context.setProperty("Script Engine", "Groovy")
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE.name, 'target/test/resources/groovy/test_log_vm_stats.groovy');

        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.getSupportedPropertyDescriptors()

        task.setup configurationContext
        task.onTrigger context
        def se = task.scriptEngine
        // This script should store a variable called x with a map of stats to values
        assertTrue se.x?.uptime >= 0
        task.offerScriptEngine(se)

    }

    class MockScriptedReportingTask extends ScriptedReportingTask implements AccessibleScriptingComponentHelper {
        def getScriptEngine() {
            return scriptingComponentHelper.engineQ.poll()
        }

        def offerScriptEngine(engine) {
            scriptingComponentHelper.engineQ.offer(engine)
        }

        @Override
        ScriptingComponentHelper getScriptingComponentHelper() {
            return this.@scriptingComponentHelper
        }
    }


}