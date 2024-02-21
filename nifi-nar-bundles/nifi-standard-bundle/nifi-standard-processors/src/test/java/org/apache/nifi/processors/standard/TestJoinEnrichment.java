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

import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.ArrayListRecordWriter;
import org.apache.nifi.serialization.record.CommaSeparatedRecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJoinEnrichment {
    private static final File EXAMPLES_DIR = new File("src/test/resources/TestJoinEnrichment");

    @Test
    public void testManyQueued() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupCsvServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_SQL);
        runner.setProperty(JoinEnrichment.SQL, "SELECT original.i, original.lower_letter, enrichment.upper_letter FROM original JOIN enrichment ON original.i = enrichment.i");

        // Enqueue a flowfile where i=0, lower_letter=a; another with i=1, lower_letter=b; etc. up to i=25, lower_letter=z
        for (int i=0; i < 26; i++) {
            final Map<String, String> originalAttributes = new HashMap<>();
            originalAttributes.put("enrichment.group.id", String.valueOf(i));
            originalAttributes.put("enrichment.role", "ORIGINAL");

            final char letter = (char) ('a' + i);

            runner.enqueue("i,lower_letter\n" + i + "," + letter, originalAttributes);
        }

        // Enqueue a flowfile where i=0, upper_letter=A; another with i=1, upper_letter=B; etc. up to i=25, upper_letter=Z
        for (int i=0; i < 26; i++) {
            final Map<String, String> enrichmentAttributes = new HashMap<>();
            enrichmentAttributes.put("enrichment.group.id", String.valueOf(i));
            enrichmentAttributes.put("enrichment.role", "ENRICHMENT");

            final char letter = (char) ('A' + i);
            runner.enqueue("i,upper_letter\n" + i + "," + letter, enrichmentAttributes);
        }

        runner.run();

        // Ensure that the result is i=0,lower_letter=a,upper_letter=A ... i=25,lower_letter=z,upper_letter=Z
        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 26);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 52);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(26, written.size());

        final BitSet found = new BitSet();
        for (final Record outRecord : written) {
            final RecordSchema schema = outRecord.getSchema();
            assertEquals(RecordFieldType.STRING, schema.getField("i").get().getDataType().getFieldType());
            assertEquals(RecordFieldType.STRING, schema.getField("lower_letter").get().getDataType().getFieldType());
            assertEquals(RecordFieldType.STRING, schema.getField("upper_letter").get().getDataType().getFieldType());

            final int id = outRecord.getAsInt("i");

            final String expectedLower = "" + ((char) ('a' + id));
            assertEquals(expectedLower, outRecord.getValue("lower_letter"));

            final String expectedUpper = "" + ((char) ('A' + id));
            assertEquals(expectedUpper, outRecord.getValue("upper_letter"));

            assertEquals(outRecord.getAsString("lower_letter"), outRecord.getAsString("upper_letter").toLowerCase());

            found.set(id);
        }

        for (int i=0; i < 26; i++) {
            assertTrue(found.get(i));
        }
    }

    @Test
    public void testSimpleSqlJoin() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupCsvServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_SQL);
        runner.setProperty(JoinEnrichment.SQL, "SELECT original.id, enrichment.name FROM original JOIN enrichment ON original.id = enrichment.id");

        final Map<String, String> originalAttributes = new HashMap<>();
        originalAttributes.put("enrichment.group.id", "abc");
        originalAttributes.put("enrichment.role", "ORIGINAL");
        runner.enqueue("id\n5", originalAttributes);

        final Map<String, String> enrichmentAttributes = new HashMap<>();
        enrichmentAttributes.put("enrichment.group.id", "abc");
        enrichmentAttributes.put("enrichment.role", "ENRICHMENT");
        runner.enqueue("id,name\n5,John Doe", enrichmentAttributes);

        runner.run();

        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 1);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 2);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record outRecord = written.getFirst();
        assertEquals(5, outRecord.getAsInt("id"));
        assertEquals("John Doe", outRecord.getValue("name"));

        final RecordSchema schema = outRecord.getSchema();
        assertEquals(RecordFieldType.STRING, schema.getField("id").get().getDataType().getFieldType());
        assertEquals(RecordFieldType.STRING, schema.getField("name").get().getDataType().getFieldType());
    }

    @Test
    public void testELReferencingAttribute() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupCsvServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_SQL);
        runner.setProperty(JoinEnrichment.SQL, "SELECT original.id, enrichment.${desired_enrichment_column} FROM original JOIN enrichment ON original.id = enrichment.id");

        final Map<String, String> originalAttributes = new HashMap<>();
        originalAttributes.put("enrichment.group.id", "abc");
        originalAttributes.put("enrichment.role", "ORIGINAL");
        runner.enqueue("id\n5", originalAttributes);

        final Map<String, String> enrichmentAttributes = new HashMap<>();
        enrichmentAttributes.put("enrichment.group.id", "abc");
        enrichmentAttributes.put("enrichment.role", "ENRICHMENT");
        enrichmentAttributes.put("desired_enrichment_column", "name");
        runner.enqueue("id,name\n5,John Doe", enrichmentAttributes);

        runner.run();

        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 1);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 2);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record outRecord = written.getFirst();
        assertEquals(5, outRecord.getAsInt("id"));
        assertEquals("John Doe", outRecord.getValue("name"));

        final RecordSchema schema = outRecord.getSchema();
        assertEquals(RecordFieldType.STRING, schema.getField("id").get().getDataType().getFieldType());
        assertEquals(RecordFieldType.STRING, schema.getField("name").get().getDataType().getFieldType());
    }


    // Tests that the LEFT OUTER JOIN example in the Additional Details works as expected
    @Test
    public void testLeftOuterJoin() throws InitializationException, IOException, SchemaNotFoundException, MalformedRecordException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupCsvServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_SQL);
        runner.setProperty(JoinEnrichment.SQL, "SELECT o.*, e.* FROM original o LEFT OUTER JOIN enrichment e ON o.id = e.customer_id");

        final Map<String, String> originalAttributes = new HashMap<>();
        originalAttributes.put("enrichment.group.id", "abc");
        originalAttributes.put("enrichment.role", "ORIGINAL");
        runner.enqueue(new File(EXAMPLES_DIR, "left-outer-join-original.csv").toPath(), originalAttributes);

        final Map<String, String> enrichmentAttributes = new HashMap<>();
        enrichmentAttributes.put("enrichment.group.id", "abc");
        enrichmentAttributes.put("enrichment.role", "ENRICHMENT");
        runner.enqueue(new File(EXAMPLES_DIR, "left-outer-join-enrichment.csv").toPath(), enrichmentAttributes);

        runner.run();

        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 1);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 2);

        final List<Record> expected = readCsvRecords(new File(EXAMPLES_DIR, "left-outer-join-expected.csv"));
        assertEquals(new HashSet<>(expected), new HashSet<>(writer.getRecordsWritten()));
    }

    // Tests that the LEFT OUTER JOIN example in the Additional Details that renames fields works as expected
    @Test
    public void testLeftOuterJoinRenameFields() throws InitializationException, IOException, SchemaNotFoundException, MalformedRecordException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupCsvServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_SQL);
        runner.setProperty(JoinEnrichment.SQL,
            "SELECT o.id, o.name, e.customer_name AS preferred_name, o.age, e.customer_email AS email FROM original o LEFT OUTER JOIN enrichment e ON o.id = e.customer_id");

        final Map<String, String> originalAttributes = new HashMap<>();
        originalAttributes.put("enrichment.group.id", "abc");
        originalAttributes.put("enrichment.role", "ORIGINAL");
        runner.enqueue(new File(EXAMPLES_DIR, "left-outer-join-original.csv").toPath(), originalAttributes);

        final Map<String, String> enrichmentAttributes = new HashMap<>();
        enrichmentAttributes.put("enrichment.group.id", "abc");
        enrichmentAttributes.put("enrichment.role", "ENRICHMENT");
        runner.enqueue(new File(EXAMPLES_DIR, "left-outer-join-enrichment.csv").toPath(), enrichmentAttributes);

        runner.run();

        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 1);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 2);

        final List<Record> expected = readCsvRecords(new File(EXAMPLES_DIR, "left-outer-join-rename-expected.csv"));
        assertEquals(new HashSet<>(expected), new HashSet<>(writer.getRecordsWritten()));
    }


    // Tests that the Insert Enrichment Record Fields example in the Additional Details produces expected output
    @Test
    public void testInsertEnrichmentFields() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupJsonServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_INSERT_ENRICHMENT_FIELDS);
        runner.setProperty(JoinEnrichment.INSERTION_RECORD_PATH, "/purchase/customer");

        final Map<String, String> originalAttributes = new HashMap<>();
        originalAttributes.put("enrichment.group.id", "abc");
        originalAttributes.put("enrichment.role", "ORIGINAL");
        runner.enqueue(new File(EXAMPLES_DIR, "insert-original.json").toPath(), originalAttributes);

        final Map<String, String> enrichmentAttributes = new HashMap<>();
        enrichmentAttributes.put("enrichment.group.id", "abc");
        enrichmentAttributes.put("enrichment.role", "ENRICHMENT");
        runner.enqueue(new File(EXAMPLES_DIR, "insert-enrichment.json").toPath(), enrichmentAttributes);

        runner.run();

        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 1);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 2);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(2, written.size());

        final RecordPath recordPath = RecordPath.compile("/purchase/customer/customerDetails");

        final List<Object> firstCustomerDetailsList = recordPath.evaluate(written.getFirst()).getSelectedFields().map(FieldValue::getValue).toList();
        assertEquals(1, firstCustomerDetailsList.size());
        final Record customerDetails = (Record) firstCustomerDetailsList.getFirst();
        assertEquals(48202, customerDetails.getValue("id"));
        assertEquals("555-555-5555", customerDetails.getValue("phone"));
        assertEquals("john.doe@nifi.apache.org", customerDetails.getValue("email"));

        final List<Object> secondCustomerDetailsList = recordPath.evaluate(written.get(1)).getSelectedFields().map(FieldValue::getValue).toList();
        assertEquals(1, secondCustomerDetailsList.size());
        final Record secondCustomerDetails = (Record) secondCustomerDetailsList.getFirst();
        assertEquals(5512, secondCustomerDetails.getValue("id"));
        assertEquals("555-555-5511", secondCustomerDetails.getValue("phone"));
        assertEquals("jane.doe@nifi.apache.org", secondCustomerDetails.getValue("email"));
    }

    // Tests that when the first enrichment record has a null value, that we still properly apply subsequent enrichments.
    @Test
    public void testFirstEnrichmentRecordNull() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new JoinEnrichment());

        final ArrayListRecordWriter writer = setupJsonServices(runner);
        runner.setProperty(JoinEnrichment.JOIN_STRATEGY, JoinEnrichment.JOIN_INSERT_ENRICHMENT_FIELDS);
        runner.setProperty(JoinEnrichment.INSERTION_RECORD_PATH, "/purchase/customer");

        final Map<String, String> originalAttributes = new HashMap<>();
        originalAttributes.put("enrichment.group.id", "abc");
        originalAttributes.put("enrichment.role", "ORIGINAL");
        runner.enqueue(new File(EXAMPLES_DIR, "insert-original.json").toPath(), originalAttributes);

        final Map<String, String> enrichmentAttributes = new HashMap<>();
        enrichmentAttributes.put("enrichment.group.id", "abc");
        enrichmentAttributes.put("enrichment.role", "ENRICHMENT");
        runner.enqueue(new File(EXAMPLES_DIR, "insert-enrichment-first-value-null.json").toPath(), enrichmentAttributes);

        runner.run();

        runner.assertTransferCount(JoinEnrichment.REL_JOINED, 1);
        runner.assertTransferCount(JoinEnrichment.REL_ORIGINAL, 2);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(2, written.size());

        final RecordPath recordPath = RecordPath.compile("/purchase/customer/customerDetails");

        final List<Object> firstCustomerDetailsList = recordPath.evaluate(written.getFirst()).getSelectedFields().map(FieldValue::getValue).toList();
        assertEquals(1, firstCustomerDetailsList.size());
        final Record customerDetails = (Record) firstCustomerDetailsList.getFirst();
        assertNull(customerDetails);

        final List<Object> secondCustomerDetailsList = recordPath.evaluate(written.get(1)).getSelectedFields().map(FieldValue::getValue).toList();
        assertEquals(1, secondCustomerDetailsList.size());
        final Record secondCustomerDetails = (Record) secondCustomerDetailsList.getFirst();
        assertEquals(5512, secondCustomerDetails.getValue("id"));
        assertEquals("555-555-5511", secondCustomerDetails.getValue("phone"));
        assertEquals("jane.doe@nifi.apache.org", secondCustomerDetails.getValue("email"));
    }


    private List<Record> readCsvRecords(final File file) throws IOException, SchemaNotFoundException, MalformedRecordException {
        final CommaSeparatedRecordReader reader = new CommaSeparatedRecordReader();
        reader.setUseNullForEmptyString(true);

        return readRecords(reader, file);
    }

    private List<Record> readRecords(final RecordReaderFactory readerFactory, final File file) throws IOException, SchemaNotFoundException, MalformedRecordException {
        final List<Record> records = new ArrayList<>();
        try (final InputStream rawIn = new FileInputStream(file);
             final InputStream in = new BufferedInputStream(rawIn)) {

            final RecordReader recordReader = readerFactory.createRecordReader(Collections.emptyMap(), in, file.length(), new MockComponentLog("id", "TestJoinEnrichment"));
            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                records.add(record);
            }
        }

        return records;
    }

    private ArrayListRecordWriter setupCsvServices(final TestRunner runner) throws InitializationException {
        final CommaSeparatedRecordReader originalReader = new CommaSeparatedRecordReader();
        originalReader.setUseNullForEmptyString(true);

        final CommaSeparatedRecordReader enrichmentReader = new CommaSeparatedRecordReader();
        enrichmentReader.setUseNullForEmptyString(true);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(null);

        runner.addControllerService("originalReader", originalReader);
        runner.enableControllerService(originalReader);
        runner.addControllerService("enrichmentReader", enrichmentReader);
        runner.enableControllerService(enrichmentReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(JoinEnrichment.ORIGINAL_RECORD_READER, "originalReader");
        runner.setProperty(JoinEnrichment.ENRICHMENT_RECORD_READER, "enrichmentReader");
        runner.setProperty(JoinEnrichment.RECORD_WRITER, "writer");

        return writer;
    }

    private ArrayListRecordWriter setupJsonServices(final TestRunner runner) throws InitializationException {
        final JsonTreeReader originalReader = new JsonTreeReader();
        final JsonTreeReader enrichmentReader = new JsonTreeReader();

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(null);

        runner.addControllerService("originalReader", originalReader);
        runner.enableControllerService(originalReader);
        runner.addControllerService("enrichmentReader", enrichmentReader);
        runner.enableControllerService(enrichmentReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(JoinEnrichment.ORIGINAL_RECORD_READER, "originalReader");
        runner.setProperty(JoinEnrichment.ENRICHMENT_RECORD_READER, "enrichmentReader");
        runner.setProperty(JoinEnrichment.RECORD_WRITER, "writer");

        return writer;
    }
}
