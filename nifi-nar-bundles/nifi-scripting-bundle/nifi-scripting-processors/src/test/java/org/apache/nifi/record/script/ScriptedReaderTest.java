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
package org.apache.nifi.record.script;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the ScriptedReader class
 */
class ScriptedReaderTest {
    private static final String SOURCE_DIR = "src/test/resources";
    private static final String GROOVY_DIR = "groovy";
    @TempDir
    private Path TARGET_DIR;
    private ScriptedReader recordReaderFactory;
    private TestRunner runner;

    @BeforeEach
    public void setUp() throws Exception {
        recordReaderFactory = new MockScriptedReader();
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });
        runner.addControllerService("reader", recordReaderFactory);
        runner.setProperty(recordReaderFactory, "Script Engine", "Groovy");
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.MODULES, (String) null);
    }

    @Test
    void testRecordReaderGroovyScript() throws Exception {
        Files.copy(Paths.get(SOURCE_DIR, GROOVY_DIR, "test_record_reader_inline.groovy"), TARGET_DIR, StandardCopyOption.REPLACE_EXISTING);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_FILE, TARGET_DIR.toString());
        runner.enableControllerService(recordReaderFactory);
        byte[] contentBytes = "Flow file content not used".getBytes();
        InputStream inStream = new ByteArrayInputStream(contentBytes);

        final RecordReader recordReader =
                recordReaderFactory.createRecordReader(Collections.emptyMap(), inStream, contentBytes.length, new MockComponentLog("id", recordReaderFactory));
        assertNotNull(recordReader);

        for(int index = 0; index < 3; index++) {
            Record record = recordReader.nextRecord();
            assertNotNull(record);
            assertEquals(record.getAsInt("code"), record.getAsInt("id") * 100);
        }
        assertNull(recordReader.nextRecord());
    }

    @Test
    void testXmlRecordReaderGroovyScript() throws Exception {
        Files.copy(Paths.get(SOURCE_DIR, GROOVY_DIR, "test_record_reader_xml.groovy"), TARGET_DIR, StandardCopyOption.REPLACE_EXISTING);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_FILE, TARGET_DIR.toString());
        String schemaText = "\n[\n{\"id\": \"int\"},\n{\"name\": \"string\"},\n{\"code\": \"int\"}\n]\n";
        runner.setProperty(recordReaderFactory, "schema.text", schemaText);
        runner.enableControllerService(recordReaderFactory);

        Map<String, String> map = new LinkedHashMap<>(1);
        map.put("record.tag", "myRecord");
        byte[] contentBytes = Files.readAllBytes(Paths.get("src/test/resources/xmlRecord.xml"));
        InputStream inStream = new ByteArrayInputStream(contentBytes);
        final RecordReader recordReader = recordReaderFactory.createRecordReader(map, inStream, contentBytes.length, new MockComponentLog("ScriptedReader", ""));
        assertNotNull(recordReader);

        for(int index = 0; index < 3; index++) {
            Record record = recordReader.nextRecord();
            assertNotNull(record);
            assertEquals(record.getAsInt("code"), record.getAsInt("id") * 100);
        }
        assertNull(recordReader.nextRecord());
    }

    @Test
    void testRecordReaderGroovyScriptChangeModuleDirectory(@TempDir Path jarDir) throws Exception {
        Files.copy(Paths.get(SOURCE_DIR, GROOVY_DIR, "test_record_reader_load_module.groovy"), TARGET_DIR, StandardCopyOption.REPLACE_EXISTING);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.SCRIPT_FILE, TARGET_DIR.toString());

        assertThrows(Throwable.class, () -> runner.enableControllerService(recordReaderFactory));

        Files.copy(Paths.get(SOURCE_DIR, "jar", "test.jar"), jarDir, StandardCopyOption.REPLACE_EXISTING);
        runner.setProperty(recordReaderFactory, ScriptingComponentUtils.MODULES, jarDir.toString());
        runner.enableControllerService(recordReaderFactory);
        byte[] contentBytes = "Flow file content not used".getBytes();
        InputStream inStream = new ByteArrayInputStream(contentBytes);

        final RecordReader recordReader =
                recordReaderFactory.createRecordReader(Collections.emptyMap(), inStream, contentBytes.length, new MockComponentLog("id", recordReaderFactory));
        assertNotNull(recordReader);
    }

    public static class MockScriptedReader extends ScriptedReader implements AccessibleScriptingComponentHelper {
        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}
