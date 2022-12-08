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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * See Javadoc {@link AbstractGoogleDriveIT} for instructions how to run this test.
 */
public class PutGoogleDriveIT extends AbstractGoogleDriveIT<PutGoogleDrive> implements OutputChecker {

    public static final String TEST_FILENAME = "testFileName";

    @BeforeEach
    public void init() throws Exception {
        super.init();
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, SHARED_FOLDER_ID);
    }

    @Override
    public PutGoogleDrive createTestSubject() {
        return new PutGoogleDrive();
    }

    @Test
    void testUploadFileToFolderById() {
        // GIVEN
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);

        // WHEN
        runWithFileContent();

        // THEN
        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
    }

    @Test
    void testUploadFileFolderByName() {
        // GIVEN
        testRunner.setProperty(PutGoogleDrive.FOLDER_NAME, "testFolderNew");
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CREATE_FOLDER, "true");

        // WHEN
        runWithFileContent();

        // THEN
        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.get(0);
        assertFlowFileAttributes(ff0);
    }

    @Test
    void testUploadFileCreateMultiLevelFolder() throws IOException {
        createFolder("existingFolder", mainFolderId);

        // GIVEN
        testRunner.setProperty(PutGoogleDrive.FOLDER_NAME, "existingFolder/new1/new2");
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CREATE_FOLDER, "true");

        // WHEN
        runWithFileContent();

        // THEN
        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.get(0);
        assertFlowFileAttributes(ff0);
    }

    @Test
    void testSpecifiedFolderIdDoesNotExist() {
        // GIVEN
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, "nonExistentId");
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, "testFile4");

        // WHEN
        runWithFileContent();

        // THEN
        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(GoogleDriveAttributes.ERROR_CODE, "404");
        ff0.assertAttributeExists(GoogleDriveAttributes.ERROR_MESSAGE);
    }

    private void runWithFileContent() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
        testRunner.enqueue(DEFAULT_FILE_CONTENT, attributes);
        testRunner.run();
    }

    private void assertFlowFileAttributes(MockFlowFile flowFile) {
        flowFile.assertAttributeExists(GoogleDriveAttributes.ID);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.FILENAME, TEST_FILENAME);
        flowFile.assertAttributeExists(GoogleDriveAttributes.TIMESTAMP);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.SIZE, String.valueOf(DEFAULT_FILE_CONTENT.length()));
        flowFile.assertAttributeEquals(GoogleDriveAttributes.MIME_TYPE, "text/plain");
    }
}
