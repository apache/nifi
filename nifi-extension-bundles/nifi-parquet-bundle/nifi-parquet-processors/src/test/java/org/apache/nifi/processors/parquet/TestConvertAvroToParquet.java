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
package org.apache.nifi.processors.parquet;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for ConvertAvroToParquet processor
 */
public class TestConvertAvroToParquet {

    private TestRunner runner;

    private final List<GenericRecord> records = new ArrayList<>();
    private File tmpAvro;
    private File tmpParquet;

    @BeforeEach
    public void setUp() throws Exception {
        tmpAvro = File.createTempFile(TestConvertAvroToParquet.class.getSimpleName(), ".avro");
        tmpAvro.deleteOnExit();

        tmpParquet = File.createTempFile(TestConvertAvroToParquet.class.getSimpleName(), ".parquet");
        tmpParquet.deleteOnExit();

        ConvertAvroToParquet processor = new ConvertAvroToParquet();
        runner = TestRunners.newTestRunner(processor);

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/avro/all-minus-enum.avsc"));

        DataFileWriter<Object> awriter = new DataFileWriter<>(new GenericDatumWriter<>());
        GenericData.Record nestedRecord = new GenericRecordBuilder(
                schema.getField("mynestedrecord").schema())
                .set("mynestedint", 1).build();

        GenericData.Record record = new GenericRecordBuilder(schema)
                .set("mynull", null)
                .set("myboolean", true)
                .set("myint", 1)
                .set("mylong", 2L)
                .set("myfloat", 3.1f)
                .set("mydouble", 4.1)
                .set("mybytes", ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)))
                .set("mystring", "hello")
                .set("mynestedrecord", nestedRecord)
                .set("myarray", new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.INT)), Arrays.asList(1, 2)))
                .set("mymap", ImmutableMap.of("a", 1, "b", 2))
                .set("myfixed", new GenericData.Fixed(Schema.createFixed("ignored", null, null, 1), new byte[] {(byte) 65}))
                .build();

        awriter.create(schema, tmpAvro);
        awriter.append(record);
        awriter.flush();
        awriter.close();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tmpAvro, datumReader);
        GenericRecord record1 = null;
        while (dataFileReader.hasNext()) {
            record1 = dataFileReader.next(record1);
            records.add(record1);
        }
    }

    @Test
    public void testProcessor() throws Exception {
        FileInputStream fileInputStream = new FileInputStream(tmpAvro);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int readedBytes;
        byte[] buf = new byte[1024];
        while ((readedBytes = fileInputStream.read(buf)) > 0) {
            out.write(buf, 0, readedBytes);
        }
        out.close();

        Map<String, String> attributes = Collections.singletonMap(CoreAttributes.FILENAME.key(), "test.avro");
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToParquet.SUCCESS, 1);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToParquet.SUCCESS).get(0);

        assertEquals("1", resultFlowFile.getAttribute(ConvertAvroToParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.parquet", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
    }

    @Test
    public void testMetaInfo() throws Exception {
        FileInputStream fileInputStream = new FileInputStream(tmpAvro);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int readedBytes;
        byte[] buf = new byte[1024];
        while ((readedBytes = fileInputStream.read(buf)) > 0) {
            out.write(buf, 0, readedBytes);
        }
        out.close();

        Map<String, String> attributes = Collections.singletonMap(CoreAttributes.FILENAME.key(), "test.avro");
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToParquet.SUCCESS).get(0);

        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream(tmpParquet);
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        final List<BlockMetaData> blocks;
        try (final ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(tmpParquet.getAbsolutePath()), conf))) {
            blocks = fileReader.getFooter().getBlocks();
        }

        long nParquetRecords = 0;
        for (final BlockMetaData meta : blocks) {
            nParquetRecords += meta.getRowCount();
        }
        long nAvroRecord = records.size();

        assertEquals(nParquetRecords, nAvroRecord);
    }

    @Test
    public void testData() throws Exception {
        FileInputStream fileInputStream = new FileInputStream(tmpAvro);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int readedBytes;
        byte[] buf = new byte[1024];
        while ((readedBytes = fileInputStream.read(buf)) > 0) {
            out.write(buf, 0, readedBytes);
        }
        out.close();

        Map<String, String> attributes = Collections.singletonMap(CoreAttributes.FILENAME.key(), "test.avro");
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToParquet.SUCCESS).get(0);

        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream(tmpParquet);
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(tmpParquet.getAbsolutePath()))
                        .withConf(conf)
                        .build();

        List<Group> parquetRecords = new ArrayList<>();

        Group current;
        current = reader.read();
        while (current != null) {
            parquetRecords.add(current);
            current = reader.read();
        }

        Group firstRecord = parquetRecords.get(0);

        // Primitive
        assertEquals(firstRecord.getInteger("myint", 0), 1);
        assertEquals(firstRecord.getLong("mylong", 0), 2);
        assertTrue(firstRecord.getBoolean("myboolean", 0));
        assertEquals(firstRecord.getFloat("myfloat", 0), 3.1, 0.0001);
        assertEquals(firstRecord.getDouble("mydouble", 0), 4.1, 0.001);
        assertEquals(firstRecord.getString("mybytes", 0), "hello");
        assertEquals(firstRecord.getString("mystring", 0), "hello");

        // Nested
        assertEquals(firstRecord.getGroup("mynestedrecord", 0).getInteger("mynestedint", 0), 1);

        // Array
        assertEquals(firstRecord.getGroup("myarray", 0).getGroup("list", 0).getInteger("element", 0), 1);
        assertEquals(firstRecord.getGroup("myarray", 0).getGroup("list", 1).getInteger("element", 0), 2);

        // Map
        assertEquals(firstRecord.getGroup("mymap", 0).getGroup("key_value", 0).getInteger("value", 0), 1);
        assertEquals(firstRecord.getGroup("mymap", 0).getGroup("key_value", 1).getInteger("value", 0), 2);

        // Fixed
        assertEquals(firstRecord.getString("myfixed", 0), "A");
    }
}