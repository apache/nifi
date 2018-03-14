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
package org.apache.nifi.processors.mongodb;


import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PutMongoRecordIT extends MongoWriteTestBase {

    private MockRecordParser recordReader;

    @Before
    public void setup() throws Exception {
        super.setup(PutMongoRecord.class);
        recordReader = new MockRecordParser();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.setProperty(PutMongoRecord.RECORD_READER_FACTORY, "reader");
    }

    @After
    public void teardown() {
        super.teardown();
    }

    private byte[] documentToByteArray(Document doc) {
        return doc.toJson().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testValidators() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PutMongoRecord.class);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        Collection<ValidationResult> results;
        ProcessContext pc;

        // missing uri, db, collection, RecordReader
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(4, results.size());
        Iterator<ValidationResult> it = results.iterator();
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo URI is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Record Reader is required"));

        // invalid write concern
        runner.setProperty(AbstractMongoProcessor.URI, MONGO_URI);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.setProperty(PutMongoRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(PutMongoRecord.WRITE_CONCERN, "xyz");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().matches("'Write Concern' .* is invalid because Given value not found in allowed set .*"));

        // valid write concern
        runner.setProperty(PutMongoRecord.WRITE_CONCERN, PutMongoRecord.WRITE_CONCERN_UNACKNOWLEDGED);
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testInsertFlatRecords() throws Exception {
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongoRecord.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongoRecord.REL_SUCCESS).get(0);


        // verify 1 doc inserted into the collection
        assertEquals(5, collection.count());
        //assertEquals(doc, collection.find().first());
    }

    @Test
    public void testInsertNestedRecords() throws Exception {
        recordReader.addSchemaField("id", RecordFieldType.INT);
        final List<RecordField> personFields = new ArrayList<>();
        final RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        final RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());
        final RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType());
        personFields.add(nameField);
        personFields.add(ageField);
        personFields.add(sportField);
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);
        recordReader.addSchemaField("person", RecordFieldType.RECORD);
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe");
            put("age", 48);
            put("sport", "Soccer");
        }}));
        recordReader.addRecord(2, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "Jane Doe");
            put("age", 47);
            put("sport", "Tennis");
        }}));
        recordReader.addRecord(3, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "Sally Doe");
            put("age", 47);
            put("sport", "Curling");
        }}));
        recordReader.addRecord(4, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "Jimmy Doe");
            put("age", 14);
            put("sport", null);
        }}));

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongoRecord.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongoRecord.REL_SUCCESS).get(0);


        // verify 1 doc inserted into the collection
        assertEquals(4, collection.count());
        //assertEquals(doc, collection.find().first());
    }
}