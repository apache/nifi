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

import static org.apache.nifi.processors.parquet.CalculateParquetOffsets.PROP_ZERO_CONTENT_OUTPUT;
import static org.apache.nifi.processors.parquet.CalculateParquetOffsets.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.nifi.parquet.ParquetTestUtils;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs({ OS.WINDOWS })
public class CalculateParquetRowGroupOffsetsTest {

    private static final Path NOT_PARQUET_PATH = Paths.get("src/test/resources/core-site.xml");

    private static final Map<String, String> PRESERVED_ATTRIBUTES = Map.of("foo", "bar", "example", "value");

    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(new CalculateParquetRowGroupOffsets());
    }

    @Test
    public void testSinglePartition() throws Exception {
        File parquetFile = ParquetTestUtils.createUsersParquetFile(10);
        runner.enqueue(parquetFile.toPath(), PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "10");
        results.getFirst().assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "4");
        results.getFirst().assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "298");
        results.getFirst().assertContentEquals(parquetFile.toPath());

        PRESERVED_ATTRIBUTES.forEach(results.getFirst()::assertAttributeEquals);
    }

    @Test
    public void testEachRowGroupGoesToSeparatePartition() throws Exception {
        File parquetFile = ParquetTestUtils.createUsersParquetFile(1000);
        runner.enqueue(parquetFile.toPath(), PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 4);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "337");
        results.getFirst().assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "4");
        results.getFirst().assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "8301");
        results.getFirst().assertContentEquals(parquetFile.toPath());

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "326");
        results.get(1).assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "8301");
        results.get(1).assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "16543");
        results.get(1).assertContentEquals(parquetFile.toPath());

        results.get(2).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "326");
        results.get(2).assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "16543");
        results.get(2).assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "24784");
        results.get(2).assertContentEquals(parquetFile.toPath());

        results.get(3).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "11");
        results.get(3).assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "24784");
        results.get(3).assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "25143");
        results.get(3).assertContentEquals(parquetFile.toPath());

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testZeroContentOutput() throws Exception {
        File parquetFile = ParquetTestUtils.createUsersParquetFile(500);
        runner.setProperty(PROP_ZERO_CONTENT_OUTPUT, "true");
        runner.enqueue(parquetFile.toPath(), PRESERVED_ATTRIBUTES);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(REL_SUCCESS);

        results.getFirst().assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "337");
        results.getFirst().assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "4");
        results.getFirst().assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "8301");
        results.getFirst().assertContentEquals("");

        results.get(1).assertAttributeEquals(ParquetAttribute.RECORD_COUNT, "163");
        results.get(1).assertAttributeEquals(ParquetAttribute.FILE_RANGE_START_OFFSET, "8301");
        results.get(1).assertAttributeEquals(ParquetAttribute.FILE_RANGE_END_OFFSET, "12468");
        results.get(1).assertContentEquals("");

        results.forEach(flowFile -> PRESERVED_ATTRIBUTES.forEach(flowFile::assertAttributeEquals));
    }

    @Test
    public void testEmptyInput() {
        runner.enqueue("", PRESERVED_ATTRIBUTES);

        final Throwable thrownException = assertThrows(Throwable.class, () -> runner.run());
        assertTrue(thrownException.getMessage().contains("is not a Parquet file"));
    }

    @Test
    public void testUnrecognizedInput() throws IOException {
        runner.enqueue(NOT_PARQUET_PATH, PRESERVED_ATTRIBUTES);

        final Throwable thrownException = assertThrows(Throwable.class, () -> runner.run());
        assertTrue(thrownException.getMessage().contains("is not a Parquet file"));
    }
}
