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
package org.apache.nifi.processors.gcp.drive;

import com.google.api.services.drive.model.File;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * See Javadoc {@link AbstractGoogleDriveIT} for instructions how to run this test.
 */
public class FetchGoogleDriveIT extends AbstractGoogleDriveIT<FetchGoogleDrive> implements OutputChecker {
    @Override
    public FetchGoogleDrive createTestSubject() {
        FetchGoogleDrive testSubject = new FetchGoogleDrive();

        return testSubject;
    }

    @Test
    void testFetchSingleFileByInputAttributes() throws Exception {
        // GIVEN
        File file = createFileWithDefaultContent("test_file.txt", mainFolderId);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("drive.id", file.getId());
        inputFlowFileAttributes.put("filename", file.getName());

        HashSet<Map<String, String>> expectedAttributes = new HashSet<>(Arrays.asList(inputFlowFileAttributes));
        List<String> expectedContent = Arrays.asList(DEFAULT_FILE_CONTENT);

        // WHEN
        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_INPUT_FAILURE, 0);

        checkAttributes(FetchGoogleDrive.REL_SUCCESS, expectedAttributes);
        checkContent(FetchGoogleDrive.REL_SUCCESS, expectedContent);
    }

    @Test
    void testInputFlowFileReferencesMissingFile() throws Exception {
        // GIVEN
        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("drive.id", "missing");
        inputFlowFileAttributes.put("filename", "missing_filename");

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(Arrays.asList(
                new HashMap<String, String>() {{
                    put("drive.id", "missing");
                    put("filename", "missing_filename");
                    put("error.code", "404");
                }}
        ));

        // WHEN
        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_INPUT_FAILURE, 0);

        checkAttributes(FetchGoogleDrive.REL_FAILURE, expectedFailureAttributes);
    }

    @Test
    void testInputFlowFileThrowsExceptionBeforeFetching() throws Exception {
        // GIVEN
        File file = createFileWithDefaultContent("test_file.txt", mainFolderId);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("drive.id", file.getId());
        inputFlowFileAttributes.put("filename", file.getName());

        MockFlowFile input = new MockFlowFile(1) {
            AtomicBoolean throwException = new AtomicBoolean(true);

            @Override
            public boolean isPenalized() {
                // We want to throw exception only once because the exception handling itself calls this again
                if (throwException.get()) {
                    throwException.set(false);
                    throw new RuntimeException("Intentional exception");
                } else {
                    return super.isPenalized();
                }
            }

            @Override
            public Map<String, String> getAttributes() {
                return inputFlowFileAttributes;
            }
        };

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(Arrays.asList(
                new HashMap<String, String>() {{
                    putAll(inputFlowFileAttributes);
                    put("error.code", "N/A");
                }}
        ));

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_INPUT_FAILURE, 0);

        checkAttributes(FetchGoogleDrive.REL_FAILURE, expectedFailureAttributes);
    }

    @Test
    void testFetchMultipleFilesByInputRecords() throws Exception {
        // GIVEN
        addJsonRecordReaderFactory();

        File file1 = createFile("test_file_1.txt", "test_content_1", mainFolderId);
        File file2 = createFile("test_file_2.txt", "test_content_2", mainFolderId);

        String input = "[" +
                "{" +
                "\"drive.id\":\"" + file1.getId() + "\"," +
                "\"filename\":\"" + file1.getName() + "\"" +
                "}," +
                "{" +
                "\"drive.id\":\"" + file2.getId() + "\"," +
                "\"filename\":\"" + file2.getName() + "\"" +
                "}" +
                "]";

        List<String> expectedContent = Arrays.asList(
                "test_content_1",
                "test_content_2"
        );

        Set<Map<String, String>> expectedAttributes = new HashSet<>(Arrays.asList(
                new HashMap<String, String>() {{
                    put("drive.id", "" + file1.getId());
                    put("filename", file1.getName());
                }},
                new HashMap<String, String>() {{
                    put("drive.id", "" + file2.getId());
                    put("filename", file2.getName());
                }}
        ));

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_INPUT_FAILURE, 0);

        checkContent(FetchGoogleDrive.REL_SUCCESS, expectedContent);
        checkAttributes(FetchGoogleDrive.REL_SUCCESS, expectedAttributes);
    }

    @Test
    void testInputRecordReferencesMissingFile() throws Exception {
        // GIVEN
        addJsonRecordReaderFactory();

        String input = "[" +
                "{" +
                "\"drive.id\":\"missing\"," +
                "\"filename\":\"missing_filename\"" +
                "}" +
                "]";

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(Arrays.asList(
                new HashMap<String, String>() {{
                    put("drive.id", "missing");
                    put("filename", "missing_filename");
                    put("error.code", "404");
                }}
        ));

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_INPUT_FAILURE, 0);

        checkAttributes(FetchGoogleDrive.REL_FAILURE, expectedFailureAttributes);
    }

    @Test
    void testInputRecordsAreInvalid() throws Exception {
        // GIVEN
        addJsonRecordReaderFactory();

        String input = "invalid json";

        List<String> expectedContents = Arrays.asList("invalid json");

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_FAILURE, 0);

        checkContent(FetchGoogleDrive.REL_INPUT_FAILURE, expectedContents);
    }

    @Test
    void testThrowExceptionBeforeRecordsAreProcessed() throws Exception {
        // GIVEN
        addJsonRecordReaderFactory();

        File file = createFile("test_file.txt", mainFolderId);

        String validInputContent = "[" +
                "{" +
                "\"drive.id\":\"" + file.getId() + "\"," +
                "\"filename\":\"" + file.getName() + "\"" +
                "}" +
                "]";

        MockFlowFile input = new MockFlowFile(1) {
            @Override
            public Map<String, String> getAttributes() {
                throw new RuntimeException("Intentional exception");
            }

            @Override
            public String getContent() {
                return validInputContent;
            }
        };

        List<String> expectedContents = Arrays.asList(validInputContent);

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchGoogleDrive.REL_FAILURE, 0);

        checkContent(FetchGoogleDrive.REL_INPUT_FAILURE, expectedContents);
    }

    @Test
    void testOneInputRecordOutOfManyThrowsUnexpectedException() throws Exception {
        // GIVEN
        AtomicReference<String> fileIdToThrowException = new AtomicReference<>();

        testSubject = new FetchGoogleDrive() {
            @Override
            void fetchFile(String fileId, ProcessSession session, FlowFile outFlowFile) throws IOException {
                if (fileId.equals(fileIdToThrowException.get())) {
                    throw new RuntimeException(fileId + " intentionally forces exception");
                }
                super.fetchFile(fileId, session, outFlowFile);
            }
        };
        testRunner = createTestRunner();

        addJsonRecordReaderFactory();

        File file1 = createFile("test_file_1.txt", "test_content_1", mainFolderId);
        File file2 = createFile("test_file_2.txt", "test_content_2", mainFolderId);

        String input = "[" +
                "{" +
                "\"drive.id\":\"" + file1.getId() + "\"," +
                "\"filename\":\"" + file1.getName() + "\"" +
                "}," +
                "{" +
                "\"drive.id\":\"" + file2.getId() + "\"," +
                "\"filename\":\"" + file2.getName() + "\"" +
                "}" +
                "]";

        fileIdToThrowException.set(file2.getId());

        Set<Map<String, String>> expectedSuccessAttributes = new HashSet<>(Arrays.asList(
                new HashMap<String, String>() {{
                    put("drive.id", file1.getId());
                    put("filename", file1.getName());
                }}
        ));
        List<String> expectedSuccessContents = Arrays.asList("test_content_1");

        Set<Map<String, String>> expectedFailureAttributes = new HashSet<>(Arrays.asList(
                new HashMap<String, String>() {{
                    put("drive.id", file2.getId());
                    put("filename", file2.getName());
                    put(FetchGoogleDrive.ERROR_CODE_ATTRIBUTE, "N/A");
                }}
        ));

        // WHEN
        testRunner.enqueue(input);
        testRunner.run();

        // THEN
        testRunner.assertTransferCount(FetchGoogleDrive.REL_INPUT_FAILURE, 0);

        checkAttributes(FetchGoogleDrive.REL_SUCCESS, expectedSuccessAttributes);
        checkContent(FetchGoogleDrive.REL_SUCCESS, expectedSuccessContents);

        checkAttributes(FetchGoogleDrive.REL_FAILURE, expectedFailureAttributes);
        checkContent(FetchGoogleDrive.REL_FAILURE, Arrays.asList(""));
    }

    private void addJsonRecordReaderFactory() throws InitializationException {
        RecordReaderFactory recordReader = new JsonTreeReader();
        testRunner.addControllerService("record_reader", recordReader);
        testRunner.enableControllerService(recordReader);
        testRunner.setProperty(FetchGoogleDrive.RECORD_READER, "record_reader");
    }

    public Set<String> getCheckedAttributeNames() {
        Set<String> checkedAttributeNames = OutputChecker.super.getCheckedAttributeNames();

        checkedAttributeNames.add(FetchGoogleDrive.ERROR_CODE_ATTRIBUTE);

        return checkedAttributeNames;
    }
}
