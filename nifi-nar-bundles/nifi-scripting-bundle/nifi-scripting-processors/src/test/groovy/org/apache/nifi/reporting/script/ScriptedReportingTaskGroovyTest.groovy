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
import org.apache.nifi.components.PropertyValue
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.provenance.ProvenanceEventBuilder
import org.apache.nifi.provenance.ProvenanceEventRecord
import org.apache.nifi.provenance.ProvenanceEventRepository
import org.apache.nifi.provenance.ProvenanceEventType
import org.apache.nifi.provenance.StandardProvenanceEventRecord
import org.apache.nifi.reporting.EventAccess
import org.apache.nifi.reporting.ReportingContext
import org.apache.nifi.reporting.ReportingInitializationContext
import org.apache.nifi.state.MockStateManager
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.MockPropertyValue
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.mockito.stubbing.Answer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.mockito.Mockito.any
import static org.mockito.Mockito.doAnswer
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when


/**
 * Unit tests for ScriptedReportingTask.
 */
@RunWith(JUnit4.class)
class ScriptedReportingTaskGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(ScriptedReportingTaskGroovyTest)
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
        def uuid = "10000000-0000-0000-0000-000000000000"
        def attributes = ['abc': 'xyz', 'xyz': 'abc', 'filename': "file-$uuid", 'uuid': uuid]
        def prevAttrs = ['filename': '1234.xyz']

        def flowFile = new MockFlowFile(3L)
        flowFile.putAttributes(attributes)
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.eventTime = System.currentTimeMillis()
        builder.eventType = ProvenanceEventType.RECEIVE
        builder.transitUri = 'nifi://unit-test'
        builder.setAttributes(prevAttrs, attributes)
        builder.componentId = '1234'
        builder.componentType = 'dummy processor'
        builder.fromFlowFile(flowFile)
        final ProvenanceEventRecord event = builder.build()

        def properties = task.supportedPropertyDescriptors.collectEntries { descriptor ->
            [descriptor: descriptor.getDefaultValue()]
        }

        // Mock the ConfigurationContext for setup(...)
        def configurationContext = mock(ConfigurationContext)
        when(configurationContext.getProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue('Groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue('target/test/resources/groovy/test_log_provenance_events.groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null))

        // Set up ReportingContext
        def context = mock(ReportingContext)
        when(context.getStateManager()).thenReturn(new MockStateManager(task))
        doAnswer({ invocation ->
            def descriptor = invocation.getArgumentAt(0, PropertyDescriptor)
            return new MockPropertyValue(properties[descriptor])
        } as Answer<PropertyValue>
        ).when(context).getProperty(any(PropertyDescriptor))


        def eventAccess = mock(EventAccess)
        // Return 3 events for the test
        doAnswer({ invocation -> return [event, event, event] } as Answer<List<ProvenanceEventRecord>>
        ).when(eventAccess).getProvenanceEvents(Mockito.anyLong(), Mockito.anyInt())

        def provenanceRepository = mock(ProvenanceEventRepository.class)
        doAnswer({ invocation -> return 3 } as Answer<Long>
        ).when(provenanceRepository).getMaxEventId()

        when(context.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository)

        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.setup configurationContext
        task.onTrigger context

        // This script should return a variable x with the number of events and a variable e with the first event
        def se = task.scriptEngine
        assertEquals 3, se.x
        assertEquals '1234', se.e.componentId
        assertEquals 'xyz', se.e.attributes.abc
        task.offerScriptEngine(se)

    }

    @Test
    void testVMEventsGroovyScript() {

        def properties = [:] as Map<PropertyDescriptor, String>
        task.getSupportedPropertyDescriptors().each { PropertyDescriptor descriptor ->
            properties.put(descriptor, descriptor.getDefaultValue())
        }

        // Mock the ConfigurationContext for setup(...)
        def configurationContext = mock(ConfigurationContext)
        when(configurationContext.getProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue('Groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue('target/test/resources/groovy/test_log_vm_stats.groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null))

        // Set up ReportingContext
        def context = mock(ReportingContext)
        when(context.getStateManager()).thenReturn(new MockStateManager(task))
        doAnswer({ invocation ->
            PropertyDescriptor descriptor = invocation.getArgumentAt(0, PropertyDescriptor)
            return new MockPropertyValue(properties[descriptor])
        } as Answer<PropertyValue>
        ).when(context).getProperty(any(PropertyDescriptor))


        def logger = mock(ComponentLog)
        def initContext = mock(ReportingInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        task.initialize initContext
        task.setup configurationContext
        task.onTrigger context
        def se = task.scriptEngine
        // This script should store a variable called x with a map of stats to values
        assertTrue se.x?.uptime > 0
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