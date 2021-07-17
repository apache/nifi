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
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.util.MockComponentLog
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import static junit.framework.TestCase.assertEquals
import static org.junit.Assert.*
/**
 * Unit tests for the ScriptedReader class
 */
@RunWith(JUnit4.class)
class ScriptedReaderTest {
    private static final String READER_INLINE_SCRIPT = "test_record_reader_inline.groovy"
    private static final String READER_XML_SCRIPT = "test_record_reader_xml.groovy"
    private static final String READER_LOAD_SCRIPT = "test_record_reader_load_module.groovy"
    private static final String TEST_JAR = "test.jar"
    private static final String SOURCE_DIR = "src/test/resources"
    private static final String GROOVY_DIR = "groovy"
    private static final String JAR_DIR = "jar"
    private static final String TARGET_DIR = "target"

    def recordReaderFactory
    def runner
    def scriptingComponent

    @Before
    void setUp() {
        recordReaderFactory = new MockScriptedReader()
        runner = TestRunners
        scriptingComponent = (AccessibleScriptingComponentHelper) recordReaderFactory
    }

    @Test
    void testRecordReaderGroovyScript() {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        Path targetPath = Paths.get(TARGET_DIR, READER_INLINE_SCRIPT)
        targetPath.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, GROOVY_DIR, READER_INLINE_SCRIPT), targetPath, StandardCopyOption.REPLACE_EXISTING)
        runner.addControllerService("reader", recordReaderFactory);
        runner.setProperty(recordReaderFactory, "Script Engine", "Groovy");
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(recordReaderFactory);

        byte[] contentBytes = 'Flow file content not used'.bytes
        InputStream inStream = new ByteArrayInputStream(contentBytes)

        RecordReader recordReader = recordReaderFactory.createRecordReader(Collections.emptyMap(), inStream, contentBytes.length,
                new MockComponentLog("id", recordReaderFactory))
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
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        Path targetPath = Paths.get(TARGET_DIR, READER_XML_SCRIPT)
        targetPath.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, GROOVY_DIR, READER_XML_SCRIPT), targetPath, StandardCopyOption.REPLACE_EXISTING)
        runner.addControllerService("reader", recordReaderFactory);
        runner.setProperty(recordReaderFactory, "Script Engine", "Groovy");
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.MODULES, (String) null);

        def schemaText = '''
                [
                  {"id": "int"},
                  {"name": "string"},
                  {"code": "int"}
                ]
            '''
        runner.setProperty(recordReaderFactory, 'schema.text', schemaText)

        def logger = new MockComponentLog('ScriptedReader', '')
        runner.enableControllerService(recordReaderFactory)

        Map<String, String> schemaVariables = ['record.tag': 'myRecord']

        byte[] contentBytes = '''
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
            '''.bytes

        InputStream inStream = new ByteArrayInputStream(contentBytes)

        RecordReader recordReader = recordReaderFactory.createRecordReader(schemaVariables, inStream, contentBytes.length, logger)
        assertNotNull(recordReader)

        3.times {
            def record = recordReader.nextRecord()
            assertNotNull(record)
            assertEquals(record.getAsInt('code'), record.getAsInt('id') * 100)
        }
        assertNull(recordReader.nextRecord())
    }

    @Test
    void testRecordReaderGroovyScriptChangeModuleDirectory() {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        Path targetPath = Paths.get(TARGET_DIR, READER_LOAD_SCRIPT)
        targetPath.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, GROOVY_DIR, READER_LOAD_SCRIPT), targetPath, StandardCopyOption.REPLACE_EXISTING)
        runner.addControllerService("reader", recordReaderFactory);
        runner.setProperty(recordReaderFactory, "Script Engine", "Groovy");
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.MODULES, (String) null);

        boolean enableFailed;
        try {
            runner.enableControllerService(recordReaderFactory);
            enableFailed = false;
        } catch (final Throwable t) {
            enableFailed = true;
            // Expected
        }
        assertTrue(enableFailed)

        Path targetJar = Paths.get(TARGET_DIR, TEST_JAR)
        targetJar.toFile().deleteOnExit()
        Files.copy(Paths.get(SOURCE_DIR, JAR_DIR, TEST_JAR), targetJar, StandardCopyOption.REPLACE_EXISTING)
        runner.setProperty(recordReaderFactory, "Module Directory", targetJar.toString());
        runner.enableControllerService(recordReaderFactory)

        byte[] contentBytes = 'Flow file content not used'.bytes
        InputStream inStream = new ByteArrayInputStream(contentBytes)

        def recordReader = recordReaderFactory.createRecordReader(Collections.emptyMap(), inStream, contentBytes.length, new MockComponentLog("id", recordReaderFactory))
        assertNotNull(recordReader)
    }

    class MockScriptedReader extends ScriptedReader implements AccessibleScriptingComponentHelper {

        @Override
        ScriptingComponentHelper getScriptingComponentHelper() {
            return this.@scriptingComponentHelper
        }
    }
}
