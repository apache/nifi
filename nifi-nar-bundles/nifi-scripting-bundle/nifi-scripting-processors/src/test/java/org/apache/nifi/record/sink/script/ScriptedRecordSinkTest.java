/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.record.sink.script;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ScriptedRecordSinkTest {

    @Test
    public void testRecordFormat() throws IOException, InitializationException {
        MockScriptedRecordSink task = initTask();

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        );
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", "Hello");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("field1", 6);
        row2.put("field2", "World!");

        RecordSet recordSet = new ListRecordSet(recordSchema, Arrays.asList(
                new MapRecord(recordSchema, row1),
                new MapRecord(recordSchema, row2)
        ));

        WriteResult writeResult = task.sendData(recordSet, new HashMap<>(), false);
        // Verify the attribute was added by the scripted RecordSinkService
        assertEquals("I am now set.", writeResult.getAttributes().get("newAttr"));
        assertEquals(2, writeResult.getRecordCount());
    }

    private MockScriptedRecordSink initTask() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        });

        final MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don"t quote values
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        final MockScriptedRecordSink recordSink = new MockScriptedRecordSink();
        runner.addControllerService("sink", recordSink);
        runner.setProperty(recordSink, "Script Engine", "Groovy");
        runner.setProperty(recordSink, ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/groovy/test_record_sink.groovy");
        runner.setProperty(recordSink, ScriptingComponentUtils.SCRIPT_BODY, (String) null);
        runner.setProperty(recordSink, ScriptingComponentUtils.MODULES, (String) null);
        runner.enableControllerService(recordSink);

        return recordSink;
    }

    public static class MockScriptedRecordSink extends ScriptedRecordSink implements AccessibleScriptingComponentHelper {

        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return super.getSupportedPropertyDescriptors();
        }
    }
}