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
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * See Javadoc {@link AbstractGoogleDriveIT} for instructions how to run this test.
 */
public class ListGoogleDriveIT extends AbstractGoogleDriveIT<ListGoogleDrive> {
    @BeforeEach
    public void init() throws Exception {
        super.init();
        testRunner.setProperty(ListGoogleDrive.FOLDER_ID, mainFolderId);
    }

    @Override
    public ListGoogleDrive createTestSubject() {
        ListGoogleDrive testSubject = new ListGoogleDrive();

        return testSubject;
    }

    @Test
    void listFilesFrom3LayerDeepDirectoryTree() throws Exception {
        File main_sub1 = createFolder("main_sub1", mainFolderId);
        File main_sub2 = createFolder("main_sub2", mainFolderId);

        File main_sub1_sub1 = createFolder("main_sub1_sub1", main_sub1.getId());

        createFileWithDefaultContent("main_file1", mainFolderId);
        createFileWithDefaultContent("main_file2", mainFolderId);
        createFileWithDefaultContent("main_file3", mainFolderId);

        createFileWithDefaultContent("main_sub1_file1", main_sub1.getId());

        createFileWithDefaultContent("main_sub2_file1", main_sub2.getId());
        createFileWithDefaultContent("main_sub2_file2", main_sub2.getId());

        createFileWithDefaultContent("main_sub1_sub1_file1", main_sub1_sub1.getId());
        createFileWithDefaultContent("main_sub1_sub1_file2", main_sub1_sub1.getId());
        createFileWithDefaultContent("main_sub1_sub1_file3", main_sub1_sub1.getId());

        Set<String> expectedFileNames = new HashSet<>(Arrays.asList(
                "main_file1", "main_file2", "main_file3",
                "main_sub1_file1",
                "main_sub2_file1", "main_sub2_file2",
                "main_sub1_sub1_file1", "main_sub1_sub1_file2", "main_sub1_sub1_file3"
        ));

        // The creation of the files are not (completely) synchronized.
        Thread.sleep(2000);

        testRunner.run();

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        Set<String> actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);

        // Next, list a sub folder, non-recursively this time. (Changing these properties will clear the Processor state as well
        //  so all files are eligible for listing again.)


        testRunner.clearTransferState();

        expectedFileNames = new HashSet<>(Arrays.asList(
                "main_sub1_file1"
        ));

        testRunner.setProperty(ListGoogleDrive.FOLDER_ID, main_sub1.getId());
        testRunner.setProperty(ListGoogleDrive.RECURSIVE_SEARCH, "false");
        testRunner.run();

        successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);
    }

    @Test
    void doNotListTooYoungFilesWhenMinAgeIsSet() throws Exception {
        testRunner.setProperty(ListGoogleDrive.MIN_AGE, "15 s");

        createFileWithDefaultContent("main_file", mainFolderId);

        // Make sure the file 'arrives' and could be listed
        Thread.sleep(5000);

        testRunner.run();

        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        Set<String> actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), actualFileNames);

        // Next, wait for another 10+ seconds for MIN_AGE to expire then list again


        Thread.sleep(10000);

        Set<String> expectedFileNames = new HashSet<>(Arrays.asList(
                "main_file"
        ));

        testRunner.run();

        successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);
    }
}
