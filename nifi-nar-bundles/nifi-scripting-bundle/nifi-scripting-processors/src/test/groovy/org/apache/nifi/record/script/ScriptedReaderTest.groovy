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
package org.apache.nifi.record.script

import org.apache.commons.io.FileUtils
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.controller.ControllerServiceInitializationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.util.MockComponentLog
import org.apache.nifi.util.MockPropertyValue
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static groovy.util.GroovyTestCase.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

/**
 * Unit tests for the ScriptedReader class
 */
@RunWith(JUnit4.class)
class ScriptedReaderTest {

    private static final Logger logger = LoggerFactory.getLogger(ScriptedReaderTest)
    def recordReaderFactory
    def runner
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
        recordReaderFactory = new MockScriptedReader()
        runner = TestRunners
        scriptingComponent = (AccessibleScriptingComponentHelper) recordReaderFactory
    }

    @Test
    void testRecordReaderGroovyScript() {

        def properties = [:] as Map<PropertyDescriptor, String>
        recordReaderFactory.getSupportedPropertyDescriptors().each {PropertyDescriptor descriptor ->
            properties.put(descriptor, descriptor.getDefaultValue())
        }

        // Mock the ConfigurationContext for setup(...)
        def configurationContext = mock(ConfigurationContext)
        when(configurationContext.getProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue('Groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue('target/test/resources/groovy/test_record_reader_inline.groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null))

        def logger = mock(ComponentLog)
        def initContext = mock(ControllerServiceInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        recordReaderFactory.initialize initContext
        recordReaderFactory.onEnabled configurationContext

        InputStream inStream = new ByteArrayInputStream('Flow file content not used'.bytes)

        RecordReader recordReader = recordReaderFactory.createRecordReader(Collections.emptyMap(), inStream, logger)
        assertNotNull(recordReader)

        3.times {
            def record = recordReader.nextRecord()
            assertNotNull(record)
            assertEquals(record.getAsInt('code'), record.getAsInt('id') * 100)
        }
        assertNull(recordReader.nextRecord())
    }

    @Test
    void testXmlRecordReaderGroovyScript() {

        def properties = [:] as Map<PropertyDescriptor, String>
        recordReaderFactory.getSupportedPropertyDescriptors().each {PropertyDescriptor descriptor ->
            properties.put(descriptor, descriptor.getDefaultValue())
        }

        // Test dynamic property descriptor
        PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
                .name('schema.text')
                .dynamic(true)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build()

        def schemaText = '''
                [
                  {"id": "int"},
                  {"name": "string"},
                  {"code": "int"}
                ]
            '''
        properties.put(SCHEMA_TEXT, schemaText)

        // Mock the ConfigurationContext for setup(...)
        def configurationContext = mock(ConfigurationContext)
        when(configurationContext.getProperties()).thenReturn(properties)
        when(configurationContext.getProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue('Groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue('target/test/resources/groovy/test_record_reader_xml.groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(SCHEMA_TEXT)).thenReturn(new MockPropertyValue(schemaText))

        def logger = new MockComponentLog('ScriptedReader', '')
        def initContext = mock(ControllerServiceInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        recordReaderFactory.initialize initContext
        recordReaderFactory.onEnabled configurationContext

        Map<String, String> schemaVariables = ['record.tag': 'myRecord']

        InputStream inStream = new ByteArrayInputStream('''
                <root>
                  <myRecord>
                    <id>1</id>
                    <name>John</name>
                    <code>100</code>
                  </myRecord>
                    <myRecord>
                    <id>2</id>
                    <name>Mary</name>
                    <code>200</code>
                  </myRecord>
                  <myRecord>
                    <id>3</id>
                    <name>Ramon</name>
                    <code>300</code>
                  </myRecord>
                </root>
            '''.bytes)

        RecordReader recordReader = recordReaderFactory.createRecordReader(schemaVariables, inStream, logger)
        assertNotNull(recordReader)

        3.times {
            def record = recordReader.nextRecord()
            assertNotNull(record)
            assertEquals(record.getAsInt('code'), record.getAsInt('id') * 100)
        }
        assertNull(recordReader.nextRecord())

    }

    class MockScriptedReader extends ScriptedReader implements AccessibleScriptingComponentHelper {

        @Override
        ScriptingComponentHelper getScriptingComponentHelper() {
            return this.@scriptingComponentHelper
        }
    }
}
