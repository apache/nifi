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
package org.apache.nifi.processors.iceberg;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.NAMESPACE;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.RECORD_READER_SERVICE;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.createTemporaryDirectory;

public class AbstractTestPutIceberg {

    protected static final String TABLE_NAME = "users";

    protected static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    protected static final org.apache.iceberg.Schema USER_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get())
    );

    protected TestRunner runner;
    protected PutIceberg processor;
    protected Catalog catalog;
    protected String warehousePath;
    protected static Schema inputSchema;

    protected void initRecordReader() throws InitializationException {
        final MockRecordParser readerFactory = new MockRecordParser();
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(inputSchema);

        for (RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField);
        }

        readerFactory.addRecord(0, "John", "Finance");
        readerFactory.addRecord(1, "Jill", "Finance");
        readerFactory.addRecord(2, "James", "Marketing");
        readerFactory.addRecord(3, "Joana", "Sales");

        runner.addControllerService(RECORD_READER_SERVICE, readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutIceberg.RECORD_READER, RECORD_READER_SERVICE);
    }

    @BeforeAll
    public static void initSchema() throws IOException {
        final String avroSchema = IOUtils.toString(Files.newInputStream(Paths.get("src/test/resources/user.avsc")), StandardCharsets.UTF_8);
        inputSchema = new Schema.Parser().parse(avroSchema);
    }

    @BeforeEach
    public void setUp() {
        warehousePath = createTemporaryDirectory().getAbsolutePath();
        processor = new PutIceberg();
    }

    @AfterEach
    public void tearDown() {
        catalog.dropTable(TABLE_IDENTIFIER);
    }
}
