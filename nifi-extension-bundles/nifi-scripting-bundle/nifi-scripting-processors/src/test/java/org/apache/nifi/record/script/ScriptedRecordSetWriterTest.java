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

import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScriptedRecordSetWriterTest {
    private static final Path GROOVY_SCRIPT_LOCATION = Paths.get("src/test/resources/groovy/test_record_writer_inline.groovy");
    private static final String GROOVY_SCRIPT_ENGINE = "Groovy";

    private static final String SERVICE_ID = ScriptedRecordSetWriterTest.class.getSimpleName();

    private static final String ID_FIELD = "id";
    private static final String LABEL_FIELD = "label";
    private static final RecordSchema RECORD_SCHEMA = new SimpleRecordSchema(
            List.of(
                    new RecordField(ID_FIELD, RecordFieldType.INT.getDataType()),
                    new RecordField(LABEL_FIELD, RecordFieldType.STRING.getDataType())
            )
    );
    private static final MapRecord[] RECORDS = new MapRecord[]{
            new MapRecord(RECORD_SCHEMA, Map.of(
                    ID_FIELD, 1,
                    LABEL_FIELD, "First Record"
            )),
            new MapRecord(RECORD_SCHEMA, Map.of(
                    ID_FIELD, 2,
                    LABEL_FIELD, "Second Record"
            ))
    };
    private static final RecordSet RECORD_SET = RecordSet.of(RECORD_SCHEMA, RECORDS);
    private static final String RECORD_TAG = "record";

    private static final String APPLICATION_XML = "application/xml";
    private static final String TEXT_XML = "text/xml";

    private final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

    @Test
    void testRecordWriterGroovyScript() throws Exception {
        final String scriptBody = Files.readString(GROOVY_SCRIPT_LOCATION);
        final ScriptedRecordSetWriter scriptedRecordSetWriter = addScriptedRecordSetWriter(scriptBody);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (
                RecordSetWriter recordSetWriter = scriptedRecordSetWriter.createWriter(runner.getLogger(), RECORD_SCHEMA, outputStream, Collections.emptyMap())
        ) {
            assertEquals(APPLICATION_XML, recordSetWriter.getMimeType());

            final WriteResult writeResult = recordSetWriter.write(RECORD_SET);
            assertEquals(RECORDS.length, writeResult.getRecordCount());
        }

        assertRecordsFound(outputStream);
    }

    @Test
    void testScriptReloaded() throws Exception {
        final String scriptBody = Files.readString(GROOVY_SCRIPT_LOCATION);
        final ScriptedRecordSetWriter scriptedRecordSetWriter = addScriptedRecordSetWriter(scriptBody);

        final RecordSetWriter recordSetWriter = scriptedRecordSetWriter.createWriter(runner.getLogger(), RECORD_SCHEMA, new ByteArrayOutputStream(), Collections.emptyMap());
        assertEquals(APPLICATION_XML, recordSetWriter.getMimeType());

        runner.disableControllerService(scriptedRecordSetWriter);
        final String scriptBodyUpdated = scriptBody.replace(APPLICATION_XML, TEXT_XML);
        runner.setProperty(scriptedRecordSetWriter, ScriptingComponentUtils.SCRIPT_BODY, scriptBodyUpdated);
        runner.enableControllerService(scriptedRecordSetWriter);

        final RecordSetWriter recordSetWriterReload = scriptedRecordSetWriter.createWriter(runner.getLogger(), RECORD_SCHEMA, new ByteArrayOutputStream(), Collections.emptyMap());
        assertEquals(TEXT_XML, recordSetWriterReload.getMimeType());
    }

    private void assertRecordsFound(final ByteArrayOutputStream outputStream) throws Exception {
        final Document document = getDocument(outputStream);
        final NodeList recordNodes = document.getElementsByTagName(RECORD_TAG);

        assertEquals(RECORDS.length, recordNodes.getLength());
    }

    private Document getDocument(final ByteArrayOutputStream outputStream) throws Exception {
        final byte[] bytes = outputStream.toByteArray();
        final DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        return documentBuilder.parse(new ByteArrayInputStream(bytes));
    }

    private ScriptedRecordSetWriter addScriptedRecordSetWriter(final String scriptBody) throws Exception {
        final ScriptedRecordSetWriter scriptedRecordSetWriter = new ScriptedRecordSetWriter();
        runner.addControllerService(SERVICE_ID, scriptedRecordSetWriter);
        runner.setProperty(scriptedRecordSetWriter, ScriptingComponentHelper.getScriptEnginePropertyBuilder().build().getName(), GROOVY_SCRIPT_ENGINE);
        runner.setProperty(scriptedRecordSetWriter, ScriptingComponentUtils.SCRIPT_BODY, scriptBody);
        runner.enableControllerService(scriptedRecordSetWriter);

        return scriptedRecordSetWriter;
    }
}
