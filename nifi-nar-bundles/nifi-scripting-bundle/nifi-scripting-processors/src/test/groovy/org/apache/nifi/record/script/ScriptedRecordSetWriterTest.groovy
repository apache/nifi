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
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.serialization.RecordSetWriter
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSet
import org.apache.nifi.util.MockPropertyValue
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertEquals
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

/**
 * Unit tests for the ScriptedReader class
 */
@RunWith(JUnit4.class)
class ScriptedRecordSetWriterTest {

    private static final Logger logger = LoggerFactory.getLogger(ScriptedRecordSetWriterTest)
    MockScriptedWriter recordSetWriterFactory
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
        recordSetWriterFactory = new MockScriptedWriter()
        runner = TestRunners
        scriptingComponent = (AccessibleScriptingComponentHelper) recordSetWriterFactory
    }

    @Test
    void testRecordWriterGroovyScript() {

        def properties = [:] as Map<PropertyDescriptor, String>
        recordSetWriterFactory.getSupportedPropertyDescriptors().each {PropertyDescriptor descriptor ->
            properties.put(descriptor, descriptor.getDefaultValue())
        }

        // Mock the ConfigurationContext for setup(...)
        def configurationContext = mock(ConfigurationContext)
        when(configurationContext.getProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue('Groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue('target/test/resources/groovy/test_record_writer_inline.groovy'))
        when(configurationContext.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null))
        when(configurationContext.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null))

        def logger = mock(ComponentLog)
        def initContext = mock(ControllerServiceInitializationContext)
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString())
        when(initContext.getLogger()).thenReturn(logger)

        recordSetWriterFactory.initialize initContext
        recordSetWriterFactory.onEnabled configurationContext

		def schema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null)
        
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(logger, schema, outputStream)
        assertNotNull(recordSetWriter)

        def recordSchema = new SimpleRecordSchema(
                [new RecordField('id', RecordFieldType.INT.dataType),
                 new RecordField('name', RecordFieldType.STRING.dataType),
                 new RecordField('code', RecordFieldType.INT.dataType)]
        )

        def records = [
                new MapRecord(recordSchema, ['id': 1, 'name': 'John', 'code': 100]),
                new MapRecord(recordSchema, ['id': 2, 'name': 'Mary', 'code': 200]),
                new MapRecord(recordSchema, ['id': 3, 'name': 'Ramon', 'code': 300])
        ] as MapRecord[]

        recordSetWriter.write(RecordSet.of(recordSchema, records))

        def xml = new XmlSlurper().parseText(outputStream.toString())
        assertEquals('1', xml.record[0].id.toString())
        assertEquals('200', xml.record[1].code.toString())
        assertEquals('Ramon', xml.record[2].name.toString())
    }

    class MockScriptedWriter extends ScriptedRecordSetWriter implements AccessibleScriptingComponentHelper {

        @Override
        ScriptingComponentHelper getScriptingComponentHelper() {
            return this.@scriptingComponentHelper
        }
    }
}
