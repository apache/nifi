/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.apache.nifi.processors.kite.TestUtil.streamFor;

public class TestInferAvroSchemaFromJSON {

    public static final Schema INPUT_SCHEMA = SchemaBuilder.record("InputTest")
            .fields().requiredString("id").requiredString("primaryColor")
            .optionalString("secondaryColor").optionalString("price")
            .endRecord();

    public static final Schema OUTPUT_SCHEMA = SchemaBuilder.record("Test")
            .fields().requiredLong("id").requiredString("color")
            .optionalDouble("price").endRecord();

    public static final String MAPPING = "[{\"source\":\"primaryColor\", \"target\":\"color\"}]";

    public static final String FAILURE_SUMMARY = "Cannot convert free to double";

    @Test
    public void testBasicConversion() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertAvroSchema.class);
        runner.assertNotValid();
        runner.setProperty(ConvertAvroSchema.INPUT_SCHEMA,
                INPUT_SCHEMA.toString());
        runner.setProperty(ConvertAvroSchema.OUTPUT_SCHEMA,
                OUTPUT_SCHEMA.toString());
        runner.setProperty("primaryColor", "color");
        runner.assertValid();

        // Two valid rows, and one invalid because "free" is not a double.
        GenericData.Record goodRecord1 = dataBasic("1", "blue", null, null);
        GenericData.Record goodRecord2 = dataBasic("2", "red", "yellow", "5.5");
        GenericData.Record badRecord = dataBasic("3", "red", "yellow", "free");
        List<GenericData.Record> input = Lists.newArrayList(goodRecord1, goodRecord2,
                badRecord);

        runner.enqueue(streamFor(input));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 2 rows", 2, converted);
        Assert.assertEquals("Should reject 1 rows", 1, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship(
                "failure").get(0);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(
                INPUT_SCHEMA);
        DataFileStream<GenericData.Record> stream = new DataFileStream<GenericData.Record>(
                new ByteArrayInputStream(
                        runner.getContentAsByteArray(incompatible)), reader);
        int count = 0;
        for (GenericData.Record r : stream) {
            Assert.assertEquals(badRecord, r);
            count++;
        }
        stream.close();
        Assert.assertEquals(1, count);
        Assert.assertEquals("Should accumulate error messages",
                FAILURE_SUMMARY, incompatible.getAttribute("errors"));

        GenericDatumReader<GenericData.Record> successReader = new GenericDatumReader<GenericData.Record>(
                OUTPUT_SCHEMA);
        DataFileStream<GenericData.Record> successStream = new DataFileStream<GenericData.Record>(
                new ByteArrayInputStream(runner.getContentAsByteArray(runner
                        .getFlowFilesForRelationship("success").get(0))),
                successReader);
        count = 0;
        for (GenericData.Record r : successStream) {
            if (count == 0) {
                Assert.assertEquals(convertBasic(goodRecord1), r);
            } else {
                Assert.assertEquals(convertBasic(goodRecord2), r);
            }
            count++;
        }
        successStream.close();
        Assert.assertEquals(2, count);
    }

    @Test
    public void testNestedConversion() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertAvroSchema.class);
        runner.assertNotValid();
        runner.setProperty(ConvertAvroSchema.INPUT_SCHEMA,
                TestAvroRecordConverter.NESTED_RECORD_SCHEMA.toString());
        runner.setProperty(ConvertAvroSchema.OUTPUT_SCHEMA,
                TestAvroRecordConverter.UNNESTED_OUTPUT_SCHEMA.toString());
        runner.setProperty("parent.id", "parentId");
        runner.assertValid();

        // Two valid rows
        GenericData.Record goodRecord1 = dataNested(1L, "200", null, null);
        GenericData.Record goodRecord2 = dataNested(2L, "300", 5L, "ParentCompany");
        List<GenericData.Record> input = Lists.newArrayList(goodRecord1, goodRecord2);

        runner.enqueue(streamFor(input));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 2 rows", 2, converted);
        Assert.assertEquals("Should reject 0 rows", 0, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);

        GenericDatumReader<GenericData.Record> successReader = new GenericDatumReader<GenericData.Record>(
                TestAvroRecordConverter.UNNESTED_OUTPUT_SCHEMA);
        DataFileStream<GenericData.Record> successStream = new DataFileStream<GenericData.Record>(
                new ByteArrayInputStream(runner.getContentAsByteArray(runner
                        .getFlowFilesForRelationship("success").get(0))),
                successReader);
        int count = 0;
        for (GenericData.Record r : successStream) {
            if (count == 0) {
                Assert.assertEquals(convertNested(goodRecord1), r);
            } else {
                Assert.assertEquals(convertNested(goodRecord2), r);
            }
            count++;
        }
        successStream.close();
        Assert.assertEquals(2, count);
    }

    private GenericData.Record convertBasic(GenericData.Record inputRecord) {
        GenericData.Record result = new GenericData.Record(OUTPUT_SCHEMA);
        result.put("id", Long.parseLong(inputRecord.get("id").toString()));
        result.put("color", inputRecord.get("primaryColor").toString());
        if (inputRecord.get("price") == null) {
            result.put("price", null);
        } else {
            result.put("price",
                    Double.parseDouble(inputRecord.get("price").toString()));
        }
        return result;
    }

    private GenericData.Record dataBasic(String id, String primaryColor,
                                         String secondaryColor, String price) {
        GenericData.Record result = new GenericData.Record(INPUT_SCHEMA);
        result.put("id", id);
        result.put("primaryColor", primaryColor);
        result.put("secondaryColor", secondaryColor);
        result.put("price", price);
        return result;
    }

    private GenericData.Record convertNested(GenericData.Record inputRecord) {
        GenericData.Record result = new GenericData.Record(
                TestAvroRecordConverter.UNNESTED_OUTPUT_SCHEMA);
        result.put("l1", inputRecord.get("l1"));
        result.put("s1", Long.parseLong(inputRecord.get("s1").toString()));
        if (inputRecord.get("parent") != null) {
            // output schema doesn't have parent name.
            result.put("parentId",
                    ((GenericData.Record) inputRecord.get("parent")).get("id"));
        }
        return result;
    }

    private GenericData.Record dataNested(long id, String companyName, Long parentId,
                                          String parentName) {
        GenericData.Record result = new GenericData.Record(TestAvroRecordConverter.NESTED_RECORD_SCHEMA);
        result.put("l1", id);
        result.put("s1", companyName);
        if (parentId != null || parentName != null) {
            GenericData.Record parent = new GenericData.Record(
                    TestAvroRecordConverter.NESTED_PARENT_SCHEMA);
            parent.put("id", parentId);
            parent.put("name", parentName);
            result.put("parent", parent);
        }
        return result;
    }
}
