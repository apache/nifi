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
package org.apache.nifi.processors.excel;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestSplitExcel {
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(SplitExcel.class);
    }

    @Test
    void testSingleSheet() throws IOException {
        Path singleSheet = Paths.get("src/test/resources/excel/dates.xlsx");
        runner.enqueue(singleSheet);

        runner.run();

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 1);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 0);
    }

    @Test
    void testMultisheet() throws IOException {
        Path multisheet = Paths.get("src/test/resources/excel/twoSheets.xlsx");
        String fileName = multisheet.toFile().getName();
        runner.enqueue(multisheet);

        runner.run();

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 2);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitExcel.REL_SPLIT);
        String expectedSheetNamesPrefix = "TestSheet";
        List<String> expectedSheetSuffixes = List.of("A", "B");
        List<Integer> expectedTotalRows = List.of(4, 3);

        for (int index = 0; index < flowFiles.size(); index++) {
            MockFlowFile flowFile = flowFiles.get(index);
            assertNotNull(flowFile.getAttribute(FRAGMENT_ID.key()));
            assertEquals(Integer.toString(index), flowFile.getAttribute(FRAGMENT_INDEX.key()));
            assertEquals(Integer.toString(flowFiles.size()), flowFile.getAttribute(FRAGMENT_COUNT.key()));
            assertEquals(fileName, flowFile.getAttribute(SEGMENT_ORIGINAL_FILENAME.key()));
            assertEquals(expectedSheetNamesPrefix + expectedSheetSuffixes.get(index), flowFile.getAttribute(SplitExcel.SHEET_NAME));
            assertEquals(expectedTotalRows.get(index).toString(), flowFile.getAttribute(SplitExcel.TOTAL_ROWS));
        }
    }

    @Test
    void testNonExcel() throws IOException {
        Path nonExcel = Paths.get("src/test/resources/excel/notExcel.txt");
        runner.enqueue(nonExcel);

        runner.run();

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 0);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 0);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 1);
    }

    @Test
    void testWithEmptySheet() throws IOException {
        Path sheetsWithEmptySheet = Paths.get("src/test/resources/excel/sheetsWithEmptySheet.xlsx");
        String fileName = sheetsWithEmptySheet.toFile().getName();
        runner.enqueue(sheetsWithEmptySheet);

        runner.run();

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 3);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitExcel.REL_SPLIT);
        List<String> expectedSheetSuffixes = List.of("TestSheetA", "TestSheetB", "emptySheet");
        List<Integer> expectedTotalRows = List.of(4, 3, 0);

        for (int index = 0; index < flowFiles.size(); index++) {
            MockFlowFile flowFile = flowFiles.get(index);
            assertNotNull(flowFile.getAttribute(FRAGMENT_ID.key()));
            assertEquals(Integer.toString(index), flowFile.getAttribute(FRAGMENT_INDEX.key()));
            assertEquals(Integer.toString(flowFiles.size()), flowFile.getAttribute(FRAGMENT_COUNT.key()));
            assertEquals(fileName, flowFile.getAttribute(SEGMENT_ORIGINAL_FILENAME.key()));
            assertEquals(expectedSheetSuffixes.get(index), flowFile.getAttribute(SplitExcel.SHEET_NAME));
            assertEquals(expectedTotalRows.get(index).toString(), flowFile.getAttribute(SplitExcel.TOTAL_ROWS));
        }
    }

    @Test
    void testDataWithSharedFormula() throws IOException {
        Path dataWithSharedFormula = Paths.get("src/test/resources/excel/dataWithSharedFormula.xlsx");
        runner.enqueue(dataWithSharedFormula);

        runner.run();

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 2);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 0);
    }
}
