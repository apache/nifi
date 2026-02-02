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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonParserFactory;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.util.JsonUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
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
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestForkRecord {

    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();
    private TestRunner runner;

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

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(ForkRecord.class);
    }

    @Test
    public void testForkExtractSimpleWithoutParentFields() throws IOException, InitializationException {
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty("my-path", "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\n42,4750.89\n43,48212.38\n");
    }

    @Test
    public void testForkExtractSimpleWithParentFields() throws IOException, InitializationException {
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\n42,John Doe,123 My Street,My City,MS,11111,USA,4750.89\n43,John Doe,123 My Street,My City,MS,11111,USA,48212.38\n");
    }

    @Test
    public void testForkExtractNotAnArray() throws IOException, InitializationException {
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/country");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testForkExtractNotAnArrayOfRecords() throws IOException, InitializationException {
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array-strings.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testForkExtractComplexWithParentFields() throws IOException, InitializationException {
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts[*]/transactions");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("""
                header
                5,John Doe,123 My Street,My City,MS,11111,USA,4750.89,150.31
                6,John Doe,123 My Street,My City,MS,11111,USA,4750.89,-15.31
                7,John Doe,123 My Street,My City,MS,11111,USA,48212.38,36.78
                8,John Doe,123 My Street,My City,MS,11111,USA,48212.38,-21.34
                """);
    }

    @Test
    public void testForkExtractComplexWithoutParentFields() throws IOException, InitializationException {
        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = new ArrayList<>();
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "false");
        runner.setProperty("my-path", "/accounts[*]/transactions");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,150.31\n6,-15.31\n7,36.78\n8,-21.34\n");
    }

    @Test
    public void testForkExtractComplexWithParentFieldsAndNull() throws IOException, InitializationException {
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
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts[*]/transactions");

        runner.enqueue(new File("src/test/resources/TestForkRecord/two-elements-nested-nested-array-null.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst();
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("""
                header
                5,John Doe,123 My Street,My City,MS,11111,USA,4750.89,150.31
                6,John Doe,123 My Street,My City,MS,11111,USA,4750.89,-15.31
                7,John Doe,123 My Street,My City,MS,11111,USA,48212.38,36.78
                8,John Doe,123 My Street,My City,MS,11111,USA,48212.38,-21.34
                """);
    }

    @Test
    public void testSplitMode() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);

        final Path schemaFile = Paths.get("src/test/resources/TestForkRecord/schema/schema.avsc");
        final String inputSchemaText = new String(Files.readAllBytes(schemaFile));
        final String outputSchemaText = new String(Files.readAllBytes(schemaFile));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);
        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final Path complexInputJson = Paths.get("src/test/resources/TestForkRecord/input/complex-input-json.json");
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");

        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_SPLIT);

        runner.setProperty("my-path", "/address");
        runner.enqueue(complexInputJson);
        runner.run();
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);
        String expectedContent = JsonUtil.getExpectedContent(Paths.get("src/test/resources/TestForkRecord/output/split-address.json"));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertContentEquals(expectedContent);
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertAttributeEquals("record.count", "5");

        runner.clearTransferState();
        runner.setProperty("my-path", "/bankAccounts[*]/last5Transactions");
        runner.enqueue(complexInputJson);
        runner.run();
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);
        expectedContent = JsonUtil.getExpectedContent(Paths.get("src/test/resources/TestForkRecord/output/split-transactions.json"));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertContentEquals(expectedContent);
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertAttributeEquals("record.count", "6");
    }

    @Test
    public void testExtractMode() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/schema/schema.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/schema/extract-schema.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);
        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");

        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.setProperty("my-path", "/bankAccounts[*]/last5Transactions");
        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json.json"));
        runner.run();
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);
        String expectedContent = JsonUtil.getExpectedContent(Paths.get("src/test/resources/TestForkRecord/output/extract-transactions.json"));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertContentEquals(expectedContent);
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertAttributeEquals("record.count", "6");
    }

    @Test
    public void testExtractWithParentFieldsAndInferredSchema() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.enableControllerService(jsonWriter);

        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("bankAccounts", "/bankAccounts");

        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json-for-inference.json"));
        runner.run();

        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final String expectedOutput = JsonUtil.getExpectedContent(Paths.get("src/test/resources/TestForkRecord/output/extract-bank-accounts-with-parents.json"));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertAttributeEquals("record.count", "5");
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertContentEquals(expectedOutput);
    }

    @Test
    public void testExtractFieldsAndInferredSchema() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.enableControllerService(jsonWriter);

        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "false");
        runner.setProperty("address", "/address");

        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json-for-inference.json"));
        runner.run();

        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final String expectedOutput = JsonUtil.getExpectedContent(Paths.get("src/test/resources/TestForkRecord/output/extract-address-without-parents.json"));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertAttributeEquals("record.count", "5");
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertContentEquals(expectedOutput);
    }

    @Test
    public void testExtractFieldsWithParentsAndFieldConflictAndInferredSchema() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.enableControllerService(jsonWriter);

        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("address", "/address");

        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json-for-inference.json"));
        runner.run();

        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final String expectedOutput = JsonUtil.getExpectedContent(Paths.get("src/test/resources/TestForkRecord/output/extract-address-with-parents.json"));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertAttributeEquals("record.count", "5");
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).getFirst().assertContentEquals(expectedOutput);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of(
                "record-reader", ForkRecord.RECORD_READER.getName(),
                "record-writer", ForkRecord.RECORD_WRITER.getName(),
                "fork-mode", ForkRecord.MODE.getName(),
                "include-parent-fields", ForkRecord.INCLUDE_PARENT_FIELDS.getName()
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private class JsonRecordReader extends AbstractControllerService implements RecordReaderFactory {

        private static final JsonParserFactory jsonParserFactory = new JsonParserFactory();

        RecordSchema schema;

        public JsonRecordReader(RecordSchema schema) {
            this.schema = schema;
        }

        @Override
        public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException {
            return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, null, null, null, null, jsonParserFactory);
        }

        @Override
        public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger)
                throws MalformedRecordException, IOException {
            return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, null, null, null, null, jsonParserFactory);
        }
    }

    private static class CustomRecordWriter extends MockRecordWriter {

        RecordSchema schema;

        public CustomRecordWriter(final String header, final boolean quoteValues, RecordSchema schema) {
            super(header, quoteValues);
            this.schema = schema;
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) {
            return this.schema;
        }

    }

}
