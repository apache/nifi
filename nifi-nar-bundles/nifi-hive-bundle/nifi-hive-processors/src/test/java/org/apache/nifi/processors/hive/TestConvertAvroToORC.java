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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.orc.TestNiFiOrcUtils;
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
import static org.junit.Assert.assertNotNull;
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
        GenericData.Record record = TestNiFiOrcUtils.buildPrimitiveAvroRecord(10, 20L, true, 30.0f, 40, StandardCharsets.UTF_8.encode("Hello"), "World");

        DatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(record.getSchema());
        DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(writer);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fileWriter.create(record.getSchema(), out);
        fileWriter.append(record);
        // Put another record in
        record = TestNiFiOrcUtils.buildPrimitiveAvroRecord(1, 2L, false, 3.0f, 4L, StandardCharsets.UTF_8.encode("I am"), "another record");
        fileWriter.append(record);
        // And one more
        record = TestNiFiOrcUtils.buildPrimitiveAvroRecord(100, 200L, true, 300.0f, 400L, StandardCharsets.UTF_8.encode("Me"), "too!");
        fileWriter.append(record);
        fileWriter.flush();
        fileWriter.close();
        out.close();
        Map<String, String> attributes = new HashMap<String, String>() {{
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
        Object o = rows.next(null);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        TypeInfo resultSchema = TestNiFiOrcUtils.buildPrimitiveOrcSchema();
        StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(resultSchema);

        // Check some fields in the first row
        Object intFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("int"));
        assertTrue(intFieldObject instanceof IntWritable);
        assertEquals(10, ((IntWritable) intFieldObject).get());
        Object stringFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("string"));
        assertTrue(stringFieldObject instanceof Text);
        assertEquals("World", stringFieldObject.toString());

    }

    @Test
    public void test_onTrigger_complex_record() throws Exception {

        Map<String, Double> mapData1 = new TreeMap<String, Double>() {{
            put("key1", 1.0);
            put("key2", 2.0);
        }};

        GenericData.Record record = TestNiFiOrcUtils.buildComplexAvroRecord(10, mapData1, "DEF", 3.0f, Arrays.asList(10, 20));

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

        record = TestNiFiOrcUtils.buildComplexAvroRecord(null, mapData2, "XYZ", 4L, Arrays.asList(100, 200));
        fileWriter.append(record);

        fileWriter.flush();
        fileWriter.close();
        out.close();

        Map<String, String> attributes = new HashMap<String, String>() {{
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
        Object o = rows.next(null);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        TypeInfo resultSchema = TestNiFiOrcUtils.buildComplexOrcSchema();
        StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(resultSchema);

        // Check some fields in the first row
        Object intFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("myInt"));
        assertTrue(intFieldObject instanceof IntWritable);
        assertEquals(10, ((IntWritable) intFieldObject).get());

        // This is pretty awkward and messy. The map object is a Map (not a MapWritable) but the keys are writables (in this case Text)
        // and so are the values (DoubleWritables in this case).
        Object mapFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("myMap"));
        assertTrue(mapFieldObject instanceof Map);
        Map map = (Map) mapFieldObject;
        Object mapValue = map.get(new Text("key1"));
        assertNotNull(mapValue);
        assertTrue(mapValue instanceof DoubleWritable);
        assertEquals(1.0, ((DoubleWritable) mapValue).get(), Double.MIN_VALUE);

        mapValue = map.get(new Text("key2"));
        assertNotNull(mapValue);
        assertTrue(mapValue instanceof DoubleWritable);
        assertEquals(2.0, ((DoubleWritable) mapValue).get(), Double.MIN_VALUE);

    }
}