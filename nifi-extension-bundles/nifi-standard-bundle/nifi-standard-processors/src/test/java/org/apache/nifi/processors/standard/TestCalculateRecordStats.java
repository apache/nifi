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

package org.apache.nifi.processors.standard;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ArrayListRecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestCalculateRecordStats {
    private TestRunner runner;
    private MockRecordParser recordParser;
    private RecordSchema personSchema;

    @BeforeEach
    void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(CalculateRecordStats.class);
        recordParser = new MockRecordParser();
        runner.addControllerService("recordReader", recordParser);
        runner.setProperty(CalculateRecordStats.RECORD_READER, "recordReader");
        runner.enableControllerService(recordParser);
        runner.assertValid();

        List<RecordField> personFields = new ArrayList<>();
        RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());
        RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType());
        personFields.add(nameField);
        personFields.add(ageField);
        personFields.add(sportField);
        personSchema = new SimpleRecordSchema(personFields);

        recordParser.addSchemaField("id", RecordFieldType.INT);
        recordParser.addSchemaField("person", RecordFieldType.RECORD);
    }

    @Test
    public void testWithArray() throws InitializationException {
        // Create a Record that has an array of records
        final List<RecordField> issueFields = new ArrayList<>();
        issueFields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        issueFields.add(new RecordField("severity", RecordFieldType.STRING.getDataType()));
        issueFields.add(new RecordField("description", RecordFieldType.STRING.getDataType()));
        final RecordSchema issueSchema = new SimpleRecordSchema(issueFields);

        final List<RecordField> issuesFields = new ArrayList<>();
        issuesFields.add(new RecordField("issues", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(issueSchema))));
        final RecordSchema issuesSchema = new SimpleRecordSchema(issuesFields);

        final List<Record> issueList = new ArrayList<>();
        issueList.add(new MapRecord(issueSchema, Map.of(
            "id", "1",
            "severity", "High",
            "description", "This is a high severity issue"
        )));
        issueList.add(new MapRecord(issueSchema, Map.of(
            "id", "2",
            "severity", "Medium",
            "description", "This is a medium severity issue"
        )));
        issueList.add(new MapRecord(issueSchema, Map.of(
            "id", "3",
            "severity", "Low",
            "description", "This is a low severity issue"
        )));
        issueList.add(new MapRecord(issueSchema, Map.of(
            "id", "",
            "severity", "High",
            "description", "This is another high severity issue"
        )));

        final Record[] issues = issueList.toArray(new Record[0]);
        final Record issuesRecord = new MapRecord(issuesSchema, Map.of(
            "issues", issues
        ));

        // Set RecordReader to one that can properly handle nested records / arrays.
        final ArrayListRecordReader readerFactory = new ArrayListRecordReader(issuesSchema);
        runner.addControllerService("readerFactory", readerFactory);
        runner.enableControllerService(readerFactory);
        runner.setProperty(CalculateRecordStats.RECORD_READER, "readerFactory");
        readerFactory.addRecord(issuesRecord);

        // Set the RecordPath to point to the 'severity' field of the record within the array.
        runner.setProperty("severity", "/issues[*]/severity");
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(CalculateRecordStats.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(CalculateRecordStats.REL_SUCCESS).getFirst();
        output.assertAttributeEquals("recordStats.severity.High", "2");
        output.assertAttributeEquals("recordStats.severity.Medium", "1");
        output.assertAttributeEquals("recordStats.severity.Low", "1");
        output.assertAttributeEquals("recordStats.severity", "4");
        output.assertAttributeEquals("record.count", "1");
    }

    @Test
    void testNoNullOrEmptyRecordFields() {
        final List<String> sports = Arrays.asList("Soccer", "Soccer", "Soccer", "Football", "Football", "Basketball");
        final Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("recordStats.sport.Soccer", "3");
        expectedAttributes.put("recordStats.sport.Football", "2");
        expectedAttributes.put("recordStats.sport.Basketball", "1");
        expectedAttributes.put("recordStats.sport", "6");
        expectedAttributes.put("record.count", "6");

        commonTest(Collections.singletonMap("sport", "/person/sport"), sports, expectedAttributes);
    }

    @Test
    void testWithNullFields() {
        final List<String> sports = Arrays.asList("Soccer", null, null, "Football", null, "Basketball");
        final Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("recordStats.sport.Soccer", "1");
        expectedAttributes.put("recordStats.sport.Football", "1");
        expectedAttributes.put("recordStats.sport.Basketball", "1");
        expectedAttributes.put("recordStats.sport", "3");
        expectedAttributes.put("record.count", "6");

        commonTest(Collections.singletonMap("sport", "/person/sport"), sports, expectedAttributes);
    }

    @Test
    void testWithFilters() {
        final List<String> sports = Arrays.asList("Soccer", "Soccer", "Soccer", "Football", "Football", "Basketball");
        final Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("recordStats.sport.Soccer", "3");
        expectedAttributes.put("recordStats.sport.Basketball", "1");
        expectedAttributes.put("recordStats.sport", "4");
        expectedAttributes.put("record.count", "6");

        final Map<String, String> propz = Collections.singletonMap("sport", "/person/sport[. != 'Football']");

        commonTest(propz, sports, expectedAttributes);
    }

    @Test
    void testWithSizeLimit() {
        runner.setProperty(CalculateRecordStats.LIMIT, "3");
        final List<String> sports = Arrays.asList("Soccer", "Soccer", "Soccer", "Football", "Football",
                "Basketball", "Baseball", "Baseball", "Baseball", "Baseball",
                "Skiing", "Skiing", "Skiing", "Snowboarding");
        final Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("recordStats.sport.Skiing", "3");
        expectedAttributes.put("recordStats.sport.Soccer", "3");
        expectedAttributes.put("recordStats.sport.Baseball", "4");
        expectedAttributes.put("recordStats.sport", String.valueOf(sports.size()));
        expectedAttributes.put("record.count", String.valueOf(sports.size()));

        final Map<String, String> counts = Collections.singletonMap("sport", "/person/sport");

        commonTest(counts, sports, expectedAttributes);
    }

    private void commonTest(Map<String, String> procProperties, List<String> sports, Map<String, String> expectedAttributes) {
        int index = 1;
        for (final String sport : sports) {
            final Map<String, Object> newRecord = new HashMap<>();
            newRecord.put("name", "John Doe");
            newRecord.put("age", 48);
            newRecord.put("sport", sport);
            recordParser.addRecord(index++, new MapRecord(personSchema, newRecord));
        }

        for (final Map.Entry<String, String> property : procProperties.entrySet()) {
            runner.setProperty(property.getKey(), property.getValue());
        }

        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(CalculateRecordStats.REL_FAILURE, 0);
        runner.assertTransferCount(CalculateRecordStats.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(CalculateRecordStats.REL_SUCCESS);
        final MockFlowFile ff = flowFiles.getFirst();
        for (final Map.Entry<String, String> expectedAttribute : expectedAttributes.entrySet()) {
            final String key = expectedAttribute.getKey();
            final String value = expectedAttribute.getValue();
            assertNotNull(ff.getAttribute(key), String.format("Missing %s", key));
            assertEquals(value, ff.getAttribute(key), "Expected " + value + " for " + key);
        }
    }
}
