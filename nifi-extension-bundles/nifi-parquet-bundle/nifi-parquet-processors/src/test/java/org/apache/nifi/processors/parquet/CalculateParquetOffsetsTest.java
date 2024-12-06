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

import static java.util.Collections.singletonMap;
import static org.apache.nifi.processors.parquet.CalculateParquetOffsets.PROP_RECORDS_PER_SPLIT;
import static org.apache.nifi.processors.parquet.CalculateParquetOffsets.PROP_ZERO_CONTENT_OUTPUT;
import static org.apache.nifi.processors.parquet.CalculateParquetOffsets.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CalculateParquetOffsetsTest {

    private static final Path PARQUET_PATH = Paths.get("src/test/resources/TestParquetReader.parquet");
    private static final Path NOT_PARQUET_PATH = Paths.get("src/test/resources/core-site.xml");

    private static final Map<String, String> PRESERVED_ATTRIBUTES = Map.of("foo", "bar", "example", "value");

    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(new CalculateParquetOffsets());
    }

    @Test
    public void testSinglePartition() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "10");
        runner.enqueue(PARQUET_PATH, PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "10");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals(PARQUET_PATH);

        PRESERVED_ATTRIBUTES.forEach(results.getFirst()::assertAttributeEquals);
    }

    @Test
    public void testEachGoesSeparatePartition() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "1");
        runner.enqueue(PARQUET_PATH, PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 10);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        for (int i = 0; i < 10; i++) {
            results.get(i).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
            results.get(i).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, String.valueOf(i));
            results.get(i).assertContentEquals(PARQUET_PATH);
        }

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testHalfPartitions() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "5");
        runner.enqueue(PARQUET_PATH, PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "5");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals(PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "5");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "5");
        results.get(1).assertContentEquals(PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testAsymmetricPartitions() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "8");
        runner.enqueue(PARQUET_PATH, PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "8");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals(PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "2");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "8");
        results.get(1).assertContentEquals(PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testSubPartitioningWithCountAndOffset() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "3");
        runner.enqueue(PARQUET_PATH, createAttributes(new HashMap<>() {
            {
                put(ParquetAttribute.RECORD_COUNT, "7");
                put(ParquetAttribute.RECORD_OFFSET, "2");
            }
        }));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 3);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "3");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "2");
        results.getFirst().assertContentEquals(PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "3");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "5");
        results.get(1).assertContentEquals(PARQUET_PATH);

        results.get(2).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
        results.get(2).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "8");
        results.get(2).assertContentEquals(PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testSubPartitioningWithoutOffset() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "2");
        runner.enqueue(PARQUET_PATH, createAttributes(singletonMap(ParquetAttribute.RECORD_COUNT, "3")));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "2");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals(PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "2");
        results.get(1).assertContentEquals(PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testSubPartitioningWithoutCount() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "5");
        runner.enqueue(PARQUET_PATH, createAttributes(singletonMap(ParquetAttribute.RECORD_OFFSET, "3")));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "5");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "3");
        results.getFirst().assertContentEquals(PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "2");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "8");
        results.get(1).assertContentEquals(PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testZeroContentOutput() throws Exception {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "8");
        runner.setProperty(PROP_ZERO_CONTENT_OUTPUT, "true");
        runner.enqueue(PARQUET_PATH, PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "8");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals("");

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "2");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "8");
        results.get(1).assertContentEquals("");

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testEmptyInput() {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "8");
        runner.enqueue("", PRESERVED_ATTRIBUTES);

        final Throwable thrownException = assertThrows(Throwable.class, () -> runner.run());
        assertTrue(thrownException.getMessage().contains("is not a Parquet file"));
    }

    @Test
    public void testEmptyInputWithOffsetAttribute() {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "8");
        runner.enqueue("", createAttributes(singletonMap(ParquetAttribute.RECORD_OFFSET, "4")));

        final Throwable thrownException = assertThrows(Throwable.class, () -> runner.run());
        assertTrue(thrownException.getMessage().contains("is not a Parquet file"));
    }

    @Test
    public void testEmptyInputWithCountAttribute() {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "3");
        runner.enqueue("", createAttributes(singletonMap(ParquetAttribute.RECORD_COUNT, "4")));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "3");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals("");

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "3");
        results.get(1).assertContentEquals("");

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testEmptyInputWithOffsetAndCountAttributes() {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "3");
        runner.enqueue("", createAttributes(new HashMap<>() {
            {
                put(ParquetAttribute.RECORD_OFFSET, "2");
                put(ParquetAttribute.RECORD_COUNT, "4");
            }
        }));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "3");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "2");
        results.getFirst().assertContentEquals("");

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "5");
        results.get(1).assertContentEquals("");

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testUnrecognizedInput() throws IOException {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "8");
        runner.enqueue(NOT_PARQUET_PATH, PRESERVED_ATTRIBUTES);

        final Throwable thrownException = assertThrows(Throwable.class, () -> runner.run());
        assertTrue(thrownException.getMessage().contains("is not a Parquet file"));
    }

    @Test
    public void testUnrecognizedInputWithOffsetAttribute() throws IOException {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "8");
        runner.enqueue(NOT_PARQUET_PATH, createAttributes(singletonMap(ParquetAttribute.RECORD_OFFSET, "4")));

        final Throwable thrownException = assertThrows(Throwable.class, () -> runner.run());
        assertTrue(thrownException.getMessage().contains("is not a Parquet file"));
    }

    @Test
    public void testUnrecognizedInputWithCountAttribute() throws IOException {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "3");
        runner.enqueue(NOT_PARQUET_PATH, createAttributes(singletonMap(ParquetAttribute.RECORD_COUNT, "4")));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "3");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "0");
        results.getFirst().assertContentEquals(NOT_PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "3");
        results.get(1).assertContentEquals(NOT_PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testUnrecognizedInputWithOffsetAndCountAttributes() throws IOException {
        runner.setProperty(PROP_RECORDS_PER_SPLIT, "3");
        runner.enqueue(NOT_PARQUET_PATH, createAttributes(new HashMap<>() {
            {
                put(ParquetAttribute.RECORD_OFFSET, "2");
                put(ParquetAttribute.RECORD_COUNT, "4");
            }
        }));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "3");
        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "2");
        results.getFirst().assertContentEquals(NOT_PARQUET_PATH);

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "1");
        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_OFFSET, "5");
        results.get(1).assertContentEquals(NOT_PARQUET_PATH);

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    private HashMap<String, String> createAttributes(Map<String, String> additionalAttributes) {
        return new HashMap<>(PRESERVED_ATTRIBUTES) {{
            putAll(additionalAttributes);
        }};
    }
}
