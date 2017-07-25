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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestForkRecord {

    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("zipCode", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("country", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getAccountSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        return new SimpleRecordSchema(accountFields);
    }

    private RecordSchema getAccountWithTransactionSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        final DataType transactionRecordType = RecordFieldType.RECORD.getRecordDataType(getTransactionSchema());
        final DataType transactionsType = RecordFieldType.ARRAY.getArrayDataType(transactionRecordType);
        accountFields.add(new RecordField("transactions", transactionsType));

        return new SimpleRecordSchema(accountFields);
    }

    private RecordSchema getTransactionSchema() {
        final List<RecordField> transactionFields = new ArrayList<>();
        transactionFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        transactionFields.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        return new SimpleRecordSchema(transactionFields);
    }

    @Test
    public void testForkSimpleWithoutParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, getAccountSchema());

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\n42,4750.89\n43,48212.38\n");
    }

    @Test
    public void testForkSimpleWithParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/accounts");
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\n42,4750.89,John Doe,123 My Street,My City,MS,11111,USA\n43,48212.38,John Doe,123 My Street,My City,MS,11111,USA\n");
    }

    @Test
    public void testForkNotAnArray() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/country");
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testForkNotAnArrayOfRecords() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.STRING.getDataType();
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/accounts");
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array-strings.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testForkComplexWithParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        fieldsWrite.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/accounts[*]/transactions");
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,150.31,John Doe,123 My Street,My City,MS,11111,USA,4750.89\n6,-15.31,John Doe,123 My Street,My City,MS,11111,USA,4750.89\n"
                + "7,36.78,John Doe,123 My Street,My City,MS,11111,USA,48212.38\n8,-21.34,John Doe,123 My Street,My City,MS,11111,USA,48212.38\n");
    }

    @Test
    public void testForkComplexWithoutParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = new ArrayList<RecordField>();
        fieldsWrite.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fieldsWrite.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/accounts[*]/transactions");
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "false");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,150.31\n6,-15.31\n7,36.78\n8,-21.34\n");
    }

    @Test
    public void testForkComplexWithParentFieldsAndNull() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        fieldsWrite.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.RECORD_PATH, "/accounts[*]/transactions");
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.enqueue(new File("src/test/resources/TestForkRecord/two-elements-nested-nested-array-null.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,150.31,John Doe,123 My Street,My City,MS,11111,USA,4750.89\n6,-15.31,John Doe,123 My Street,My City,MS,11111,USA,4750.89\n"
                + "7,36.78,John Doe,123 My Street,My City,MS,11111,USA,48212.38\n8,-21.34,John Doe,123 My Street,My City,MS,11111,USA,48212.38\n");
    }

    private class JsonRecordReader extends AbstractControllerService implements RecordReaderFactory {

        RecordSchema schema;

        public JsonRecordReader(RecordSchema schema) {
            this.schema = schema;
        }

        @Override
        public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
            return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat);
        }

    }

    private class CustomRecordWriter extends MockRecordWriter {

        RecordSchema schema;

        public CustomRecordWriter(final String header, final boolean quoteValues, RecordSchema schema) {
            super(header, quoteValues);
            this.schema = schema;
        }

        @Override
        public RecordSchema getSchema(FlowFile flowFile, RecordSchema schema) throws SchemaNotFoundException, IOException {
            return this.schema;
        }

    }

}
