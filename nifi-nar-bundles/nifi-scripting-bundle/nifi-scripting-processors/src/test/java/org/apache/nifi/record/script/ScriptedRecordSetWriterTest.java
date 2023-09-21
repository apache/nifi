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
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for the ScriptedReader class
 */
public class ScriptedRecordSetWriterTest {
    @TempDir
    private static Path targetPath;

    @BeforeAll
    public static void setUpOnce() throws Exception {
        Files.copy(Paths.get("src/test/resources/groovy/test_record_writer_inline.groovy"), targetPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    void testRecordWriterGroovyScript() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        MockScriptedWriter recordSetWriterFactory = new MockScriptedWriter();
        runner.addControllerService("writer", recordSetWriterFactory);
        runner.setProperty(recordSetWriterFactory, "Script Engine", "Groovy");
        runner.setProperty(recordSetWriterFactory, ScriptingComponentUtils.SCRIPT_FILE, targetPath.toString());
        runner.setProperty(recordSetWriterFactory, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordSetWriterFactory, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(recordSetWriterFactory);

        RecordSchema schema = recordSetWriterFactory.getSchema(Collections.emptyMap(), null);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(new MockComponentLog("id", recordSetWriterFactory), schema, outputStream, Collections.emptyMap());
        assertNotNull(recordSetWriter);

        SimpleRecordSchema recordSchema = new SimpleRecordSchema(Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("code", RecordFieldType.INT.getDataType())));
        MapRecord [] records = createMapRecords(recordSchema);

        recordSetWriter.write(RecordSet.of(recordSchema, records));

        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = documentBuilder.parse(new ByteArrayInputStream(outputStream.toByteArray()));
        XPathFactory xpathfactory = XPathFactory.newInstance();
        XPath xpath = xpathfactory.newXPath();
        assertEquals("1", xpath.evaluate("//record[1]/id/text()", document));
        assertEquals("200", xpath.evaluate("//record[2]/code/text()", document));
        assertEquals("Ramon", xpath.evaluate("//record[3]/name/text()", document));
    }

    private static MapRecord[] createMapRecords(SimpleRecordSchema recordSchema) {
        Map<String, Object> map = new LinkedHashMap<>(3);
        map.put("id", 1);
        map.put("name", "John");
        map.put("code", 100);
        Map<String, Object> map1 = new LinkedHashMap<>(3);
        map1.put("id", 2);
        map1.put("name", "Mary");
        map1.put("code", 200);
        Map<String, Object> map2 = new LinkedHashMap<>(3);
        map2.put("id", 3);
        map2.put("name", "Ramon");
        map2.put("code", 300);

        return new MapRecord[]{new MapRecord(recordSchema, map), new MapRecord(recordSchema, map1), new MapRecord(recordSchema, map2)};
    }
    public static class MockScriptedWriter extends ScriptedRecordSetWriter implements AccessibleScriptingComponentHelper {
        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}
