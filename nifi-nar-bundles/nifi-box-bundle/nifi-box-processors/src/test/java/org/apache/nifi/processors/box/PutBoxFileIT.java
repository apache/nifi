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
package org.apache.nifi.processors.box;

import org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy;
import org.junit.jupiter.api.Test;

/**
 * See Javadoc {@link AbstractBoxFileIT} for instructions how to run this test.
 */
public class PutBoxFileIT extends AbstractBoxFileIT<PutBoxFile>{

    @Override
    public PutBoxFile createTestSubject() {
        return new PutBoxFile();
    }

    @Test
    void testUploadFile() {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.enqueue(DEFAULT_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(FetchBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(FetchBoxFile.REL_FAILURE, 0);
    }

    @Test
    void testSubfoldersAreCreated()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutBoxFile.SUBFOLDER_NAME, "sub1/sub2/sub3");
        testRunner.setProperty(PutBoxFile.CREATE_SUBFOLDER, "true");
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.enqueue(DEFAULT_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(FetchBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(FetchBoxFile.REL_FAILURE, 0);
    }

    @Test
    void testSubfolderExists()  {
        createFolder("sub1", mainFolderId);

        testRunner.setProperty(PutBoxFile.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutBoxFile.SUBFOLDER_NAME, "sub1/sub2/sub3");
        testRunner.setProperty(PutBoxFile.CREATE_SUBFOLDER, "true");
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.enqueue(DEFAULT_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(FetchBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(FetchBoxFile.REL_FAILURE, 0);
    }

    @Test
    void testUploadExistingFileFailResolution()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.enqueue(DEFAULT_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutBoxFile.REL_FAILURE, 0);
        testRunner.clearTransferState();
        testRunner.enqueue(CHANGED_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutBoxFile.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutBoxFile.REL_FAILURE, 1);
    }

    @Test
    void testUploadExistingFileIgnoreResolution()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutBoxFile.CONFLICT_RESOLUTION, ConflictResolutionStrategy.IGNORE.getValue());

        testRunner.enqueue(DEFAULT_FILE_CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutBoxFile.REL_FAILURE, 0);
        testRunner.clearTransferState();

        testRunner.enqueue(CHANGED_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutBoxFile.REL_FAILURE, 0);
    }

    @Test
    void testUploadExistingFileReplaceResolution()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, mainFolderId);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutBoxFile.CONFLICT_RESOLUTION, ConflictResolutionStrategy.REPLACE.getValue());

        testRunner.enqueue(DEFAULT_FILE_CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutBoxFile.REL_FAILURE, 0);
        testRunner.clearTransferState();

        testRunner.enqueue(CHANGED_FILE_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutBoxFile.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutBoxFile.REL_FAILURE, 0);
    }
}
