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

package org.apache.nifi.processors.cassandra;

import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processors.cassandra.mock.MockCQLQueryExecutionService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExecuteCQLQueryRecordTest {
    private TestRunner testRunner;

    @BeforeEach
    public void setup() {
        testRunner = TestRunners.newTestRunner(ExecuteCQLQueryRecord.class);
        testRunner.setProperty(ExecuteCQLQueryRecord.CQL_SELECT_QUERY, "SELECT * FROM cql_table");
    }

    @DisplayName("Verify the normal write behavior with different record counts and expected batch sizes")
    @ParameterizedTest(name = "rows per file={0},generated records={1},expected ff count={2}")
    @CsvSource({
        "5,4,1,0",
        "5,100,20,0",
        "5,101,21,0",
        "5,100,20,1"
    })
    public void testSimpleWriteScenario(int rowsPerFlowFile, int recordCount,
                                        int expectedFlowFileCount,
                                        int outputBatchSize) throws Exception {
        testRunner.setProperty(ExecuteCQLQueryRecord.MAX_ROWS_PER_FLOW_FILE, String.valueOf(rowsPerFlowFile));
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_BATCH_SIZE, String.valueOf(outputBatchSize));

        MockRecordWriter parser = new MockRecordWriter();
        RecordField field1 = new RecordField("a", RecordFieldType.STRING.getDataType());
        RecordField field2 = new RecordField("b", RecordFieldType.STRING.getDataType());
        SimpleRecordSchema schema = new SimpleRecordSchema(List.of(field1, field2));

        ArrayList<Record> data = new ArrayList<>();

        for (int i = 0; i < recordCount; i++) {
            Map<String, Object> rec = Map.of("a", UUID.randomUUID().toString(), "b", UUID.randomUUID().toString());
            MapRecord testData = new MapRecord(schema, rec);
            data.add(testData);
        }

        MockCQLQueryExecutionService service = new MockCQLQueryExecutionService(data.iterator());

        testRunner.setProperty(ExecuteCQLQueryRecord.CONNECTION_PROVIDER_SERVICE, "connection");
        testRunner.addControllerService("connection", service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(ExecuteCQLQueryRecord.OUTPUT_WRITER, "writer");
        testRunner.addControllerService("writer", parser);
        testRunner.enableControllerService(parser);
        testRunner.assertValid();

        testRunner.enqueue("parent_file");
        testRunner.run();

        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(ExecuteCQLQueryRecord.REL_SUCCESS, expectedFlowFileCount);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ExecuteCQLQueryRecord.REL_SUCCESS);
        assertEquals(expectedFlowFileCount, flowFiles.size());
        flowFiles.forEach(ff -> {
            assertNotNull(ff.getAttribute(FragmentAttributes.FRAGMENT_ID.key()));
            assertNotNull(ff.getAttribute(FragmentAttributes.FRAGMENT_COUNT.key()));
            assertNotNull(ff.getAttribute(FragmentAttributes.FRAGMENT_INDEX.key()));

            int fragmentIndex = Integer.parseInt(ff.getAttribute(FragmentAttributes.FRAGMENT_INDEX.key()));
            int rowCount = Integer.parseInt(ff.getAttribute(FragmentAttributes.FRAGMENT_COUNT.key()));
            String fragmentId = ff.getAttribute(FragmentAttributes.FRAGMENT_ID.key());

            assertTrue(fragmentIndex < expectedFlowFileCount);
            assertTrue(rowCount > 0 && rowCount <= rowsPerFlowFile);

            assertDoesNotThrow(() -> UUID.fromString(fragmentId));
        });
    }
}
