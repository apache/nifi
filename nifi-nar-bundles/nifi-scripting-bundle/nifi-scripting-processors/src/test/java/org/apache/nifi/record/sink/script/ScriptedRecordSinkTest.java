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

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.script.AccessibleScriptingComponentHelper;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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

        final MockScriptedRecordSink recordSink = new MockScriptedRecordSink();
        ConfigurationContext context = mock(ConfigurationContext.class);
        StateManager stateManager = new MockStateManager(recordSink);

        final PropertyValue pValue = mock(StandardPropertyValue.class);
        MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don"t quote values
        when(context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY)).thenReturn(pValue);
        when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer);


        final ComponentLog logger = mock(ComponentLog.class);
        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(writer, UUID.randomUUID().toString(), logger, stateManager);
        recordSink.initialize(initContext);

        // Call something that sets up the ScriptingComponentHelper, so we can mock it
        recordSink.getSupportedPropertyDescriptors();

        when(context.getProperty(recordSink.getScriptingComponentHelper().SCRIPT_ENGINE))
                .thenReturn(new MockPropertyValue("Groovy"));
        when(context.getProperty(ScriptingComponentUtils.SCRIPT_FILE))
                .thenReturn(new MockPropertyValue("src/test/resources/groovy/test_record_sink.groovy"));
        when(context.getProperty(ScriptingComponentUtils.SCRIPT_BODY))
                .thenReturn(new MockPropertyValue(null));
        when(context.getProperty(ScriptingComponentUtils.MODULES))
                .thenReturn(new MockPropertyValue(null));
        try {
            recordSink.onEnabled(context);
        } catch (Exception e) {
            e.printStackTrace();
            fail("onEnabled error: " + e.getMessage());
        }
        return recordSink;
    }

    public static class MockScriptedRecordSink extends ScriptedRecordSink implements AccessibleScriptingComponentHelper {

        @Override
        public ScriptingComponentHelper getScriptingComponentHelper() {
            return this.scriptingComponentHelper;
        }
    }
}