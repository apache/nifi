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
package org.apache.nifi.processors.hive;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.orc.TestOrcUtils;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Unit tests for ConvertAvroToORC processor
 */
public class TestConvertAvroToORC {

    private ConvertAvroToORC processor;
    private TestRunner runner;

    @Before
    public void setUp() throws Exception {
        processor = new ConvertAvroToORC();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void test_Setup() throws Exception {

    }

    @Test
    public void test_onTrigger_primitive_record() throws Exception {
        GenericData.Record record = TestOrcUtils.buildPrimitiveAvroRecord(10, 20L, true, 30.0f, 40, StandardCharsets.UTF_8.encode("Hello"), "World");

        DatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(record.getSchema());
        DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(writer);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fileWriter.create(record.getSchema(), out);
        fileWriter.append(record);
        // Put another record in
        record = TestOrcUtils.buildPrimitiveAvroRecord(1, 2L, false, 3.0f, 4L, StandardCharsets.UTF_8.encode("I am"), "another record");
        fileWriter.append(record);
        // And one more
        record = TestOrcUtils.buildPrimitiveAvroRecord(100, 200L, true, 300.0f, 400L, StandardCharsets.UTF_8.encode("Me"), "too!");
        fileWriter.append(record);
        fileWriter.flush();
        fileWriter.close();
        out.close();
        Map<String,String> attributes = new HashMap<String,String>(){{
            put(CoreAttributes.FILENAME.key(), "test.avro");
        }};
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToORC.REL_SUCCESS, 1);

        // Write the flow file out to disk, since the ORC Reader needs a path
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS test_record (int INT, long BIGINT, boolean BOOLEAN, float FLOAT, double DOUBLE, bytes BINARY, string STRING)"
                + " STORED AS ORC", resultFlowFile.getAttribute(ConvertAvroToORC.HIVE_DDL_ATTRIBUTE));
        assertEquals("3", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"), OrcFile.readerOptions(conf).filesystem(fs));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        assertTrue(rows.nextBatch(batch));
        assertTrue(batch.cols[0] instanceof LongColumnVector);
        assertEquals(10, ((LongColumnVector) batch.cols[0]).vector[0]);
        assertEquals(1, ((LongColumnVector) batch.cols[0]).vector[1]);
        assertEquals(100, ((LongColumnVector) batch.cols[0]).vector[2]);
        assertTrue(batch.cols[1] instanceof LongColumnVector);
        assertEquals(20, ((LongColumnVector) batch.cols[1]).vector[0]);
        assertEquals(2, ((LongColumnVector) batch.cols[1]).vector[1]);
        assertEquals(200, ((LongColumnVector) batch.cols[1]).vector[2]);
        assertTrue(batch.cols[2] instanceof LongColumnVector);
        assertEquals(1, ((LongColumnVector) batch.cols[2]).vector[0]);
        assertEquals(0, ((LongColumnVector) batch.cols[2]).vector[1]);
        assertEquals(1, ((LongColumnVector) batch.cols[2]).vector[2]);
        assertTrue(batch.cols[3] instanceof DoubleColumnVector);
        assertEquals(30.0f, ((DoubleColumnVector) batch.cols[3]).vector[0], Double.MIN_NORMAL);
        assertEquals(3.0f, ((DoubleColumnVector) batch.cols[3]).vector[1], Double.MIN_NORMAL);
        assertEquals(300.0f, ((DoubleColumnVector) batch.cols[3]).vector[2], Double.MIN_NORMAL);
        assertTrue(batch.cols[4] instanceof DoubleColumnVector);
        assertEquals(40.0f, ((DoubleColumnVector) batch.cols[4]).vector[0], Double.MIN_NORMAL);
        assertEquals(4.0f, ((DoubleColumnVector) batch.cols[4]).vector[1], Double.MIN_NORMAL);
        assertEquals(400.0f, ((DoubleColumnVector) batch.cols[4]).vector[2], Double.MIN_NORMAL);
        assertTrue(batch.cols[5] instanceof BytesColumnVector);
        assertEquals("Hello", ((BytesColumnVector) batch.cols[5]).toString(0));
        assertEquals("I am", ((BytesColumnVector) batch.cols[5]).toString(1));
        assertEquals("Me", ((BytesColumnVector) batch.cols[5]).toString(2));
        assertTrue(batch.cols[6] instanceof BytesColumnVector);
        assertEquals("World", ((BytesColumnVector) batch.cols[6]).toString(0));
        assertEquals("another record", ((BytesColumnVector) batch.cols[6]).toString(1));
        assertEquals("too!", ((BytesColumnVector) batch.cols[6]).toString(2));
    }

    @Test
    public void test_onTrigger_complex_record() throws Exception {

        Map<String, Double> mapData1 = new TreeMap<String, Double>() {{
            put("key1", 1.0);
            put("key2", 2.0);
        }};

        GenericData.Record record = TestOrcUtils.buildComplexAvroRecord(10, mapData1, "DEF", 3.0f, Arrays.asList(10, 20));

        DatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(record.getSchema());
        DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(writer);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fileWriter.create(record.getSchema(), out);
        fileWriter.append(record);

        // Put another record in
        Map<String, Double> mapData2 = new TreeMap<String, Double>() {{
            put("key1", 3.0);
            put("key2", 4.0);
        }};

        record = TestOrcUtils.buildComplexAvroRecord(null, mapData2, "XYZ", 4L, Arrays.asList(100, 200));
        fileWriter.append(record);

        fileWriter.flush();
        fileWriter.close();
        out.close();

        Map<String,String> attributes = new HashMap<String,String>(){{
            put(CoreAttributes.FILENAME.key(), "test");
        }};
        runner.enqueue(out.toByteArray(), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToORC.REL_SUCCESS, 1);

        // Write the flow file out to disk, since the ORC Reader needs a path
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS complex_record " +
                "(myInt INT, myMap MAP<STRING, DOUBLE>, myEnum STRING, myLongOrFloat UNIONTYPE<BIGINT, FLOAT>, myIntList ARRAY<INT>)"
                + " STORED AS ORC", resultFlowFile.getAttribute(ConvertAvroToORC.HIVE_DDL_ATTRIBUTE));
        assertEquals("2", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"), OrcFile.readerOptions(conf).filesystem(fs));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        assertTrue(rows.nextBatch(batch));
        assertTrue(batch.cols[0] instanceof LongColumnVector);
        assertEquals(10, ((LongColumnVector) batch.cols[0]).vector[0]);
        assertTrue(batch.cols[1] instanceof MapColumnVector);
        assertTrue(batch.cols[2] instanceof BytesColumnVector);
        assertTrue(batch.cols[3] instanceof UnionColumnVector);
        StringBuilder buffer = new StringBuilder();
        batch.cols[3].stringifyValue(buffer, 1);
        assertEquals("{\"tag\": 0, \"value\": 4}", buffer.toString());
        assertTrue(batch.cols[4] instanceof ListColumnVector);
    }

    @Test
    public void test_onTrigger_multiple_batches() throws Exception {

        Schema recordSchema = TestOrcUtils.buildPrimitiveAvroSchema();
        DatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(recordSchema);
        DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(writer);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fileWriter.create(recordSchema, out);

        GenericData.Record record;
        for (int i = 1;i<=2000;i++) {
            record = TestOrcUtils.buildPrimitiveAvroRecord(i, 2L * i, true, 30.0f * i, 40L * i, StandardCharsets.UTF_8.encode("Hello"), "World");


            fileWriter.append(record);
        }

        fileWriter.flush();
        fileWriter.close();
        out.close();
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToORC.REL_SUCCESS, 1);

        // Write the flow file out to disk, since the ORC Reader needs a path
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("2000", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"), OrcFile.readerOptions(conf).filesystem(fs));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        assertTrue(rows.nextBatch(batch));
        // At least 2 batches were created
        assertTrue(rows.nextBatch(batch));
    }
}