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

package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PutElasticsearchHttpRecordIT {
    protected TestRunner runner;
    private MockRecordParser recordReader;
    static RecordSchema personSchema;
    static TestRunner FETCH_RUNNER;
    ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public static void setupTests() throws Exception {
        final List<RecordField> personFields = new ArrayList<>();
        final RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        final RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());
        final RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType());
        personFields.add(nameField);
        personFields.add(ageField);
        personFields.add(sportField);
        personSchema = new SimpleRecordSchema(personFields);

        FETCH_RUNNER = TestRunners.newTestRunner(FetchElasticsearchHttp.class);
        FETCH_RUNNER.setProperty(FetchElasticsearchHttp.ES_URL, "http://localhost:9200");
        FETCH_RUNNER.setProperty(FetchElasticsearchHttp.INDEX, "people_test");
        FETCH_RUNNER.setProperty(FetchElasticsearchHttp.TYPE, "person");
        FETCH_RUNNER.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        FETCH_RUNNER.assertValid();
    }

    @Before
    public void setup() throws Exception {
        recordReader = new MockRecordParser();
        recordReader.addSchemaField("id", RecordFieldType.INT);

        recordReader.addSchemaField("person", RecordFieldType.RECORD);

        runner = TestRunners.newTestRunner(PutElasticsearchHttpRecord.class);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_READER, "reader");
        runner.setProperty(PutElasticsearchHttpRecord.ES_URL, "http://localhost:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "people_test");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "person");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.assertValid();
    }

    @After
    public void tearDown() {
        FETCH_RUNNER.clearTransferState();
    }

    private void setupPut() {
        runner.enqueue("");
        runner.run(1, true, true);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
    }

    private void testFetch(List<Map<String, String>> attrs) {
        for (Map<String, String> attr : attrs) {
            FETCH_RUNNER.enqueue("", attr);
        }

        FETCH_RUNNER.run(attrs.size(), true, true);
        FETCH_RUNNER.assertTransferCount(FetchElasticsearchHttp.REL_FAILURE, 0);
        FETCH_RUNNER.assertTransferCount(FetchElasticsearchHttp.REL_RETRY, 0);
        FETCH_RUNNER.assertTransferCount(FetchElasticsearchHttp.REL_NOT_FOUND, 0);
        FETCH_RUNNER.assertTransferCount(FetchElasticsearchHttp.REL_SUCCESS, attrs.size());
    }

    @Test
    public void testNoNullSuppresion() throws Exception {
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", null);
        }}));

        List<Map<String, String>> attrs = new ArrayList<>();
        Map<String, String> attr = new HashMap<>();
        attr.put("doc_id", "1");
        attrs.add(attr);

        setupPut();
        testFetch(attrs);

        byte[] raw = FETCH_RUNNER.getContentAsByteArray(FETCH_RUNNER.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0));
        String val = new String(raw);

        Map<String, Object> parsed = mapper.readValue(val, Map.class);
        Assert.assertNotNull(parsed);
        Map<String, Object> person = (Map)parsed.get("person");
        Assert.assertNotNull(person);
        Assert.assertTrue(person.containsKey("sport"));
        Assert.assertNull(person.get("sport"));
    }

    private void sharedSuppressTest(SharedPostTest spt) throws Exception {
        List<Map<String, String>> attrs = new ArrayList<>();
        Map<String, String> attr = new HashMap<>();
        attr.put("doc_id", "1");
        attrs.add(attr);
        attr = new HashMap<>();
        attr.put("doc_id", "2");
        attrs.add(attr);

        setupPut();
        testFetch(attrs);

        List<MockFlowFile> flowFiles = FETCH_RUNNER.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS);
        String ff1 = new String(FETCH_RUNNER.getContentAsByteArray(flowFiles.get(0)));
        String ff2 = new String(FETCH_RUNNER.getContentAsByteArray(flowFiles.get(1)));
        Map<String, Object> ff1Map = mapper.readValue(ff1, Map.class);
        Map<String, Object> ff2Map = mapper.readValue(ff2, Map.class);
        Assert.assertNotNull(ff1Map);
        Assert.assertNotNull(ff2Map);
        Map<String, Object> p1 = (Map)ff1Map.get("person");
        Map<String, Object> p2 = (Map)ff2Map.get("person");
        Assert.assertNotNull(p1);
        Assert.assertNotNull(p2);

        spt.run(p1, p2);
    }

    @Test
    public void testMissingRecord() throws Exception {
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
        }}));

        recordReader.addRecord(2, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", null);
        }}));

        runner.setProperty(PutElasticsearchHttpRecord.SUPPRESS_NULLS, PutElasticsearchHttpRecord.SUPPRESS_MISSING);

        sharedSuppressTest((p1, p2) -> {
            Assert.assertFalse(p1.containsKey("sport"));
            Assert.assertTrue(p2.containsKey("sport"));
            Assert.assertNull(p2.get("sport"));
        });
    }

    @Test
    public void testAlwaysSuppress() throws Exception {
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
        }}));

        recordReader.addRecord(2, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", null);
        }}));

        runner.setProperty(PutElasticsearchHttpRecord.SUPPRESS_NULLS, PutElasticsearchHttpRecord.ALWAYS_SUPPRESS);

        sharedSuppressTest((p1, p2) -> {
            Assert.assertFalse(p1.containsKey("sport"));
            Assert.assertFalse(p2.containsKey("sport"));
        });
    }

    @Test
    public void testIllegalIndexName() throws Exception {
        // Undo some stuff from setup()
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "people\"test");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "person");
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", null);
        }}));

        List<Map<String, String>> attrs = new ArrayList<>();
        Map<String, String> attr = new HashMap<>();
        attr.put("doc_id", "1");
        attrs.add(attr);

        runner.enqueue("");
        runner.run(1, true, true);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testIndexNameWithJsonChar() throws Exception {
        // Undo some stuff from setup()
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "people}test");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "person");
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", null);
        }}));

        List<Map<String, String>> attrs = new ArrayList<>();
        Map<String, String> attr = new HashMap<>();
        attr.put("doc_id", "1");
        attrs.add(attr);

        runner.enqueue("");
        runner.run(1, true, true);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testTypeNameWithSpecialChars() throws Exception {
        // Undo some stuff from setup()
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "people_test2");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "per\"son");
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", null);
        }}));

        List<Map<String, String>> attrs = new ArrayList<>();
        Map<String, String> attr = new HashMap<>();
        attr.put("doc_id", "1");
        attrs.add(attr);

        setupPut();
    }

    private interface SharedPostTest {
        void run(Map<String, Object> p1, Map<String, Object> p2);
    }
}
