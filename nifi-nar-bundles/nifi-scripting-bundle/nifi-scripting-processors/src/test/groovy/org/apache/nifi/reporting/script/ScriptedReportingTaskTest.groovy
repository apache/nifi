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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.mockito.Mockito.*
/**
 * Unit tests for ScriptedReportingTask.
 */
@RunWith(JUnit4.class)
class ScriptedReportingTaskTest {
    private static final String PROVENANCE_EVENTS_SCRIPT = "test_log_provenance_events.groovy"
    private static final String LOG_VM_STATS = "test_log_vm_stats.groovy"
    private static final String SOURCE_DIR = "src/test/resources/groovy"
    private static final String TARGET_DIR = "target"

    def task
    def runner
    def scriptingComponent

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

        Path targetPath = Paths.get(TARGET_DIR, PROVENANCE_EVENTS_SCRIPT)
        targetPath.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, PROVENANCE_EVENTS_SCRIPT), targetPath, StandardCopyOption.REPLACE_EXISTING)
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null)

        final MockReportingContext context = new MockReportingContext([:], null, VariableRegistry.EMPTY_REGISTRY)
        context.setProperty("Script Engine", "Groovy")
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE.name, targetPath.toString())

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
        def sr = task.scriptRunner
        def se = sr.scriptEngine
        assertEquals 3, se.x
        assertEquals '1234', se.e.componentId
        assertEquals 'xyz', se.e.attributes.abc
        task.offerScriptRunner(sr)
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

        Path targetPath = Paths.get(TARGET_DIR, LOG_VM_STATS)
        targetPath.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, LOG_VM_STATS), targetPath, StandardCopyOption.REPLACE_EXISTING)
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null)

        final MockReportingContext context = new MockReportingContext([:], null, VariableRegistry.EMPTY_REGISTRY)
        context.setProperty("Script Engine", "Groovy")
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE.name, targetPath.toString());

        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.getSupportedPropertyDescriptors()

        task.setup configurationContext
        task.onTrigger context
        def sr = task.scriptRunner
        def se = sr.scriptEngine
        // This script should store a variable called x with a map of stats to values
        assertTrue se.x?.uptime >= 0
        task.offerScriptRunner(sr)

    }

    @Test
    void testVMEventsJythonScript() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(new PropertyDescriptor.Builder().name("Script Engine").build(), "Groovy");

        Path targetPath = Paths.get(TARGET_DIR, LOG_VM_STATS)
        targetPath.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, LOG_VM_STATS), targetPath, StandardCopyOption.REPLACE_EXISTING)
        properties.put(ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null)

        final MockReportingContext context = new MockReportingContext([:], null, VariableRegistry.EMPTY_REGISTRY)
        context.setProperty("Script Engine", "Groovy")
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE.name, targetPath.toString());

        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.getSupportedPropertyDescriptors()

        task.setup configurationContext
        task.onTrigger context
        def sr = task.scriptRunner
        def se = sr.scriptEngine
        // This script should store a variable called x with a map of stats to values
        assertTrue se.x?.uptime >= 0
        task.offerScriptRunner(sr)

    }

    class MockScriptedReportingTask extends ScriptedReportingTask implements AccessibleScriptingComponentHelper {
        def getScriptRunner() {
            return scriptingComponentHelper.scriptRunnerQ.poll()
        }

        def offerScriptRunner(runner) {
            scriptingComponentHelper.scriptRunnerQ.offer(runner)
        }

        @Override
        ScriptingComponentHelper getScriptingComponentHelper() {
            return this.@scriptingComponentHelper
        }
    }


}