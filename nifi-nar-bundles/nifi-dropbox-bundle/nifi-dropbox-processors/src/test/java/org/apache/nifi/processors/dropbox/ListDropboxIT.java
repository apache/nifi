/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.dropbox;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListDropboxIT extends AbstractDropboxIT<ListDropbox> {

    private static final String NOT_MAIN_FOLDER = "/notMainFolder";
    private static final String YOUNG_FILE_NAME = "just_created";

    @BeforeEach
    public void init() throws Exception {
        super.init();
        testRunner.setProperty(ListDropbox.FOLDER, MAIN_FOLDER);
    }

    @AfterEach
    public void teardown() throws Exception {
        super.teardown();
        deleteFolderIfExists(NOT_MAIN_FOLDER);
    }

    @Override
    protected ListDropbox createTestSubject() {
        return new ListDropbox();
    }

    @Test
    void testEmbeddedDirectoriesAreListed() throws Exception {
        createFile(MAIN_FOLDER, "test_file1", "test_file_content1");
        createFile(MAIN_FOLDER, "test_file2", "test_file_content2");
        createFile(MAIN_FOLDER + "/testFolder1", "test_file11", "test_file_content11");
        createFile(MAIN_FOLDER + "/testFolder2", "test_file112", "test_file_content112");

        createFile(NOT_MAIN_FOLDER, "test_file_not_in_main_folder", "test_file_content31");

        List<String> expectedFileNames = Arrays.asList("test_file1", "test_file2", "test_file11", "test_file112");

        waitForFileCreation();

        testRunner.run();

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        List<String> actualFileNames = getFilenames(successFlowFiles);

        assertEquals(expectedFileNames, actualFileNames);
    }

    @Test
    void testFolderIsListedById() throws Exception {
        testRunner.setProperty(ListDropbox.FOLDER, mainFolderId);

        createFile(MAIN_FOLDER, "test_file1", "test_file_content1");
        createFile(MAIN_FOLDER + "/testFolder1", "test_file11", "test_file_content11");

        List<String> expectedFileNames = Arrays.asList("test_file1", "test_file11");

        waitForFileCreation();

        testRunner.run();

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        List<String> actualFileNames = getFilenames(successFlowFiles);

        assertEquals(expectedFileNames, actualFileNames);
    }

    @Test
    void testTooYoungFilesNotListedWhenMinAgeIsSet() throws Exception {
        testRunner.setProperty(ListDropbox.MIN_AGE, "15 s");

        createFile(MAIN_FOLDER, YOUNG_FILE_NAME, "test_file_content1");

        waitForFileCreation();

        testRunner.run();

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        List<String> actualFileNames = getFilenames(successFlowFiles);

        assertEquals(emptyList(), actualFileNames);

        // Next, wait for another 10+ seconds for MIN_AGE to expire then list again.
        Thread.sleep(10000);

        List<String> expectedFileNames = singletonList(YOUNG_FILE_NAME);

        testRunner.run();

        successFlowFiles = testRunner.getFlowFilesForRelationship(ListDropbox.REL_SUCCESS);

        actualFileNames = getFilenames(successFlowFiles);

        assertEquals(expectedFileNames, actualFileNames);
    }

    private void waitForFileCreation() throws InterruptedException {
        // We need to wait since the creation of the files are not (completely) synchronized.
        Thread.sleep(5000);
    }

    private List<String> getFilenames(List<MockFlowFile> flowFiles) {
        return flowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(toList());
    }
}
