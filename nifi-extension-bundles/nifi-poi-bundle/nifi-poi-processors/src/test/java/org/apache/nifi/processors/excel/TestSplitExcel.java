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

import org.apache.nifi.excel.ProtectionType;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSplitExcel {
    private static final String PASSWORD = "nifi";
    private static final ByteArrayOutputStream PASSWORD_PROTECTED = new ByteArrayOutputStream();
    private TestRunner runner;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            workbook.createSheet("User Info");
            workbook.createSheet("Vehicle Info");
            workbook.write(outputStream);
        }

        //Protect the Excel file with a password
        try (POIFSFileSystem poifsFileSystem = new POIFSFileSystem()) {
            EncryptionInfo encryptionInfo = new EncryptionInfo(EncryptionMode.agile);
            Encryptor encryptor = encryptionInfo.getEncryptor();
            encryptor.confirmPassword(PASSWORD);

            try (OPCPackage opc = OPCPackage.open(new ByteArrayInputStream(outputStream.toByteArray()));
                 OutputStream os = encryptor.getDataStream(poifsFileSystem)) {
                opc.save(os);
            }
            poifsFileSystem.writeFilesystem(PASSWORD_PROTECTED);
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

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 0);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 0);

        MockComponentLog logger = runner.getLogger();
        assertTrue(logger.getWarnMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().contains("nothing to split")));
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
            assertNotNull(flowFile.getAttribute(SplitExcel.FRAGMENT_ID));
            assertEquals(Integer.toString(index), flowFile.getAttribute(SplitExcel.FRAGMENT_INDEX));
            assertEquals(Integer.toString(flowFiles.size()), flowFile.getAttribute(SplitExcel.FRAGMENT_COUNT));
            assertEquals(fileName, flowFile.getAttribute(SplitExcel.SEGMENT_ORIGINAL_FILENAME));
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
            assertNotNull(flowFile.getAttribute(SplitExcel.FRAGMENT_ID));
            assertEquals(Integer.toString(index), flowFile.getAttribute(SplitExcel.FRAGMENT_INDEX));
            assertEquals(Integer.toString(flowFiles.size()), flowFile.getAttribute(SplitExcel.FRAGMENT_COUNT));
            assertEquals(fileName, flowFile.getAttribute(SplitExcel.SEGMENT_ORIGINAL_FILENAME));
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

    @Test
    void testPasswordProtected() {
        runner.setProperty(SplitExcel.PROTECTION_TYPE, ProtectionType.PASSWORD.getValue());
        runner.setProperty(SplitExcel.PASSWORD, PASSWORD);
        runner.setProperty(SplitExcel.AVOID_TEMP_FILES, Boolean.TRUE.toString());
        runner.enqueue(PASSWORD_PROTECTED.toByteArray());

        runner.run();

        runner.assertTransferCount(SplitExcel.REL_SPLIT, 2);
        runner.assertTransferCount(SplitExcel.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitExcel.REL_FAILURE, 0);
    }
}
