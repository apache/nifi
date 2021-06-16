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

package org.apache.nifi.rules.handlers;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class TestRecordSinkHandler {
    private TestRunner runner;
    private MockComponentLog mockComponentLog;
    private RecordSinkHandler recordSinkHandler;
    private MockRecordSinkService recordSinkService;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        mockComponentLog = new MockComponentLog();
        RecordSinkHandler handler = new MockRecordSinkHandler(mockComponentLog);
        recordSinkService = new MockRecordSinkService();
        runner.addControllerService("MockRecordSinkService", recordSinkService);
        runner.enableControllerService(recordSinkService);
        runner.addControllerService("MockRecordSinkHandler", handler);
        runner.setProperty(handler, MockRecordSinkHandler.RECORD_SINK_SERVICE,"MockRecordSinkService");
        runner.enableControllerService(handler);
        recordSinkHandler = (RecordSinkHandler) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("MockRecordSinkHandler");
    }

    @Test
    public void testValidService() {
        runner.assertValid(recordSinkHandler);
        assertThat(recordSinkHandler, instanceOf(RecordSinkHandler.class));
    }

    @Test
    public void testRecordSendViaSink() throws InitializationException, IOException {
        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "Records written to sink service:";
        final BigDecimal bigDecimalValue = new BigDecimal(String.join("", Collections.nCopies(400, "1")) + ".2");

        attributes.put("sendZeroResults","false");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");
        metrics.put("custom", bigDecimalValue);

        final Action action = new Action();
        action.setType("SEND");
        action.setAttributes(attributes);
        recordSinkHandler.execute(action, metrics);
        String logMessage = mockComponentLog.getDebugMessage();
        List<Map<String, Object>> rows = recordSinkService.getRows();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertTrue(logMessage.startsWith(expectedMessage));
        assertFalse(rows.isEmpty());
        Map<String,Object> record = rows.get(0);
        assertEquals("90", (record.get("cpu")));
        assertEquals("1000000", (record.get("jvmHeap")));
        assertEquals(bigDecimalValue, (record.get("custom")));
    }

    private static class MockRecordSinkHandler extends RecordSinkHandler {
        private ComponentLog testLogger;

        public MockRecordSinkHandler(ComponentLog testLogger) {
            this.testLogger = testLogger;
        }

        @Override
        protected ComponentLog getLogger() {
            return testLogger;
        }
    }

    private static class MockRecordSinkService extends AbstractConfigurableComponent implements RecordSinkService {

        private List<Map<String, Object>> rows = new ArrayList<>();

        @Override
        public WriteResult sendData(RecordSet recordSet, Map<String,String> attributes, boolean sendZeroResults) throws IOException {
            int numRecordsWritten = 0;
            RecordSchema recordSchema = recordSet.getSchema();
            Record record;
            while ((record = recordSet.next()) != null) {
                Map<String, Object> row = new HashMap<>();
                final Record finalRecord = record;
                recordSchema.getFieldNames().forEach((fieldName) -> row.put(fieldName, finalRecord.getValue(fieldName)));
                rows.add(row);
                numRecordsWritten++;
            }
            return WriteResult.of(numRecordsWritten, Collections.emptyMap());
        }

        @Override
        public String getIdentifier() {
            return "MockRecordSinkService";
        }

        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {
        }

        public List<Map<String, Object>> getRows() {
            return rows;
        }
    }


}
