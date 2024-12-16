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

import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;
import static org.apache.nifi.processors.gcp.drive.PutGoogleDrive.FILE_NAME;
import static org.apache.nifi.processors.gcp.drive.PutGoogleDrive.FOLDER_ID;

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
    }

    @Override
    public PutGoogleDrive createTestSubject() {
        return new PutGoogleDrive();
    }

    @Test
    void testUploadFileToFolderById() {
        testRunner.setProperty(FOLDER_ID, mainFolderId);
        testRunner.setProperty(FILE_NAME, TEST_FILENAME);

        runWithFileContent();

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertFlowFileAttributes(ff0);
    }

    @Test
    void testUploadedFileAlreadyExistsFailResolution() {
        testRunner.setProperty(FOLDER_ID, mainFolderId);
        testRunner.setProperty(FILE_NAME, TEST_FILENAME);

        runWithFileContent();

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
        testRunner.clearTransferState();

        runWithFileContent();

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 1);

    }

    @Test
    void testUploadedFileAlreadyExistsReplaceResolution() {
        testRunner.setProperty(FOLDER_ID, mainFolderId);
        testRunner.setProperty(FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CONFLICT_RESOLUTION, REPLACE.getValue());

        runWithFileContent();

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
        testRunner.clearTransferState();

        runWithFileContent("012345678");

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(GoogleDriveAttributes.SIZE, "9");
    }

    @Test
    void testUploadedFileAlreadyExistsIgnoreResolution() {
        testRunner.setProperty(FOLDER_ID, mainFolderId);
        testRunner.setProperty(FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CONFLICT_RESOLUTION, IGNORE.getValue());

        runWithFileContent();

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
        testRunner.clearTransferState();

        runWithFileContent();

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
    }

    @Test
    void testChunkedUpload() {
        testRunner.setProperty(FOLDER_ID, mainFolderId);
        testRunner.setProperty(FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "256 KB");
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_THRESHOLD, "300 KB");

        runWithFileContent(LARGE_FILE_CONTENT);

        testRunner.assertTransferCount(PutGoogleDrive.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGoogleDrive.REL_FAILURE, 0);
    }


    private void runWithFileContent() {
       runWithFileContent(DEFAULT_FILE_CONTENT);
    }

    private void runWithFileContent(String content) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
        testRunner.enqueue(content, attributes);
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
