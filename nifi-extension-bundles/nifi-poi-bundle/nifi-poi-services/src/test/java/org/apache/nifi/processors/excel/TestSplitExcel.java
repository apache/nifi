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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSplitExcel {
    private TestRunner runner;

    /*
     * Cleanup the temporary poifiles directory which is created by org.apache.poi.util.DefaultTempFileCreationStrategy
     * the strategy org.apache.poi.util.TempFile uses which in turn is used by com.github.pjfanning.xlsx.impl.StreamingSheetReader.
     */
    @AfterAll
    public static void cleanUpAfterAll() {
        final Path tempDir = Path.of(System.getProperty("java.io.tmpdir")).resolve("poifiles");
        try (DirectoryStream<Path> directoryStream = newDirectoryStream(tempDir, "tmp-[0-9]*.xlsx")) {
            for (Path tmpFile : directoryStream) {
                Files.deleteIfExists(tmpFile);
            }
        } catch (Exception ignored) {
        }
    }

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

        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(SplitExcel.REL_SPLIT)) {
            try (XSSFWorkbook workbook = new XSSFWorkbook(flowFile.getContentStream())) {
                Sheet firstSheet = workbook.sheetIterator().next();

                // Start from the second row as the first row has column header names
                List<Cell> formulaCells = Stream.iterate(firstSheet.getFirstRowNum() + 1, rowIndex -> rowIndex + 1)
                        .limit(firstSheet.getLastRowNum())
                        .map(firstSheet::getRow)
                        .filter(Objects::nonNull)
                        .map(row -> row.getCell(7)) // NOTE: The argument is 0 based although the formula column when viewed in Excel is in the 8th column.
                        .filter(Objects::nonNull)
                        .toList();

                for (Cell formulaCell : formulaCells) {
                    Row row = formulaCell.getRow();
                    Sheet sheet = row.getSheet();
                    String messagePrefix = String.format("Cell %s in row %s in sheet %s",
                            formulaCell.getColumnIndex(), row.getRowNum(), sheet.getSheetName());

                    // If copy cell formula is set to true the cell types would be FORMULA and the numeric value would be 0.0.
                    assertEquals(CellType.NUMERIC, formulaCell.getCellType(), String.format("%s did not have the expected NUMERIC cell type", messagePrefix));
                    assertTrue(formulaCell.getNumericCellValue() > 0.0, String.format("%s did not have expected numeric value greater than 0.0", messagePrefix));
                }
            }
        }
    }
}