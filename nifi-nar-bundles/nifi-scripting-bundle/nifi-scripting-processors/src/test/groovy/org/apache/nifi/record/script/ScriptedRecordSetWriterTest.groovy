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

import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentHelper
import org.apache.nifi.script.ScriptingComponentUtils
import org.apache.nifi.serialization.RecordSetWriter
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSet
import org.apache.nifi.util.MockComponentLog
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull

/**
 * Unit tests for the ScriptedReader class
 */
@RunWith(JUnit4.class)
class ScriptedRecordSetWriterTest {

    private static final Logger logger = LoggerFactory.getLogger(ScriptedRecordSetWriterTest)
    private static final String INLINE_GROOVY_PATH = "test_record_writer_inline.groovy"
    private static final String SOURCE_DIR = "src/test/resources/groovy"
    private static final Path TARGET_PATH = Paths.get("target", INLINE_GROOVY_PATH)
    MockScriptedWriter recordSetWriterFactory
    def runner
    def scriptingComponent


    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = {String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
        Files.copy(Paths.get(SOURCE_DIR, INLINE_GROOVY_PATH), TARGET_PATH, StandardCopyOption.REPLACE_EXISTING)
        TARGET_PATH.toFile().deleteOnExit()
    }

    @Before
    void setUp() {
        recordSetWriterFactory = new MockScriptedWriter()
        runner = TestRunners
        scriptingComponent = (AccessibleScriptingComponentHelper) recordSetWriterFactory
    }

    @Test
    void testRecordWriterGroovyScript() {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        runner.addControllerService("writer", recordSetWriterFactory);
        runner.setProperty(recordSetWriterFactory, "Script Engine", "Groovy");
        runner.setProperty(recordSetWriterFactory, ScriptingComponentUtils.SCRIPT_FILE, TARGET_PATH.toString());
        runner.setProperty(recordSetWriterFactory, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordSetWriterFactory, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(recordSetWriterFactory);

		def schema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null)

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(new MockComponentLog('id', recordSetWriterFactory), schema, outputStream, Collections.emptyMap())
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
