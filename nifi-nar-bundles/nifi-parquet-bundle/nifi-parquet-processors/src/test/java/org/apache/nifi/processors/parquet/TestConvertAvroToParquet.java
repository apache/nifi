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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ConvertAvroToParquet processor
 */
public class TestConvertAvroToParquet {

    private ConvertAvroToParquet processor;
    private TestRunner runner;

    private List<GenericRecord> records = new ArrayList<>();
    File tmpAvro = new File("target/test.avro");
    File tmpParquet = new File("target/test.parquet");

    @Before
    public void setUp() throws Exception {
        processor = new ConvertAvroToParquet();
        runner = TestRunners.newTestRunner(processor);

        Schema schema = new Schema.Parser().parse(Resources.getResource("avro/all-minus-enum.avsc").openStream());

        DataFileWriter<Object> awriter = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
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
                .set("mybytes", ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)))
                .set("mystring", "hello")
                .set("mynestedrecord", nestedRecord)
                .set("myarray", new GenericData.Array<Integer>(Schema.createArray(Schema.create(Schema.Type.INT)), Arrays.asList(1, 2)))
                .set("mymap", ImmutableMap.of("a", 1, "b", 2))
                .set("myfixed", new GenericData.Fixed(Schema.createFixed("ignored", null, null, 1), new byte[] { (byte) 65 }))
                .build();

        awriter.create(schema, tmpAvro);
        awriter.append(record);
        awriter.flush();
        awriter.close();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmpAvro, datumReader);
        GenericRecord record1 = null;
        while (dataFileReader.hasNext()) {
            record1 = dataFileReader.next(record1);
            records.add(record1);
        }

    }

    @Test
    public void test_Processor() throws Exception {

        FileInputStream fileInputStream = new FileInputStream(tmpAvro);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int readedBytes;
        byte[] buf = new byte[1024];
        while ((readedBytes = fileInputStream.read(buf)) > 0) {
            out.write(buf, 0, readedBytes);
        }
        out.close();

        Map<String, String> attributes = new HashMap<String, String>() {{
            put(CoreAttributes.FILENAME.key(), "test.avro");
        }};
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToParquet.SUCCESS, 1);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToParquet.SUCCESS).get(0);

        // assert meta data
        assertEquals("1", resultFlowFile.getAttribute(ConvertAvroToParquet.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.parquet", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));


    }

    @Test
    public void test_Meta_Info() throws Exception {

        FileInputStream fileInputStream = new FileInputStream(tmpAvro);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int readedBytes;
        byte[] buf = new byte[1024];
        while ((readedBytes = fileInputStream.read(buf)) > 0) {
            out.write(buf, 0, readedBytes);
        }
        out.close();

        Map<String, String> attributes = new HashMap<String, String>() {{
            put(CoreAttributes.FILENAME.key(), "test.avro");
        }};
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToParquet.SUCCESS).get(0);

        // Save the flowfile
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream(tmpParquet);
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        ParquetMetadata metaData;
        metaData = ParquetFileReader.readFooter(conf, new Path(tmpParquet.getAbsolutePath()), NO_FILTER);

        // #number of records
        long nParquetRecords = 0;
        for(BlockMetaData meta : metaData.getBlocks()){
            nParquetRecords += meta.getRowCount();
        }
        long nAvroRecord = records.size();

        assertEquals(nParquetRecords, nAvroRecord);
    }

    @Test
    public void test_Data() throws Exception {


        FileInputStream fileInputStream = new FileInputStream(tmpAvro);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int readedBytes;
        byte[] buf = new byte[1024];
        while ((readedBytes = fileInputStream.read(buf)) > 0) {
            out.write(buf, 0, readedBytes);
        }
        out.close();

        Map<String, String> attributes = new HashMap<String, String>() {{
            put(CoreAttributes.FILENAME.key(), "test.avro");
        }};
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToParquet.SUCCESS).get(0);

        // Save the flowfile
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream(tmpParquet);
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(tmpParquet.getAbsolutePath()))
                        .withConf(conf)
                        .build();

        List<Group> parquetRecords = new ArrayList<Group>();

        Group current;
        current = reader.read();
        while (current != null) {
            assertTrue(current instanceof Group);
            parquetRecords.add(current);
            current = reader.read();
        }

        Group firstRecord = parquetRecords.get(0);

        // Primitive
        assertEquals(firstRecord.getInteger("myint", 0), 1);
        assertEquals(firstRecord.getLong("mylong", 0), 2);
        assertEquals(firstRecord.getBoolean("myboolean", 0), true);
        assertEquals(firstRecord.getFloat("myfloat", 0), 3.1, 0.0001);
        assertEquals(firstRecord.getDouble("mydouble", 0), 4.1, 0.001);
        assertEquals(firstRecord.getString("mybytes", 0), "hello");
        assertEquals(firstRecord.getString("mystring", 0), "hello");

        // Nested
        assertEquals(firstRecord.getGroup("mynestedrecord",0).getInteger("mynestedint",0), 1);

        // Array
        assertEquals(firstRecord.getGroup("myarray",0).getGroup("list",0).getInteger("element", 0), 1);
        assertEquals(firstRecord.getGroup("myarray",0).getGroup("list",1).getInteger("element", 0), 2);

        // Map
        assertEquals(firstRecord.getGroup("mymap",0).getGroup("map",0).getInteger("value", 0), 1);
        assertEquals(firstRecord.getGroup("mymap",0).getGroup("map",1).getInteger("value", 0), 2);

        // Fixed
        assertEquals(firstRecord.getString("myfixed",0), "A");

    }

    @After
    public void cleanup(){
        tmpAvro.delete();
        tmpParquet.delete();

    }


}