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

import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.FAIL;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PutDropboxIT extends AbstractDropboxIT<PutDropbox> {

    private static final String CONTENT = "content";
    private static final String CHANGED_CONTENT = "changedContent";
    private static final String NON_EXISTING_FOLDER = "/doesNotExistYet";

    @BeforeEach
    public void init() throws Exception {
        super.init();
        testRunner.setProperty(PutDropbox.FILE_NAME, "testFile.json");
    }

    @AfterEach
    public void teardown() throws Exception {
        super.teardown();
        deleteFolderIfExists(NON_EXISTING_FOLDER);
    }

    @Override
    protected PutDropbox createTestSubject() {
        return new PutDropbox();
    }

    @Test
    void testUploadFileToExistingDirectory()  {
        testRunner.setProperty(PutDropbox.FOLDER, MAIN_FOLDER);

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
    }

    @Test
    void testUploadFileCreateFolderWithSubFolders()  {
        testRunner.setProperty(PutDropbox.FOLDER, NON_EXISTING_FOLDER + "/subFolder1/subFolder2");

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
    }

    @Test
    void testEmptyFileIsUpladed()  {
        testRunner.setProperty(PutDropbox.FOLDER, MAIN_FOLDER);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
    }

    @Test
    void testUploadExistingFileFailStrategy()  {
        testRunner.setProperty(PutDropbox.FOLDER, MAIN_FOLDER);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, FAIL.getValue());

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
        testRunner.clearTransferState();

        testRunner.enqueue(CHANGED_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 1);
    }

    @Test
    void testUploadExistingFileWithSameContentFailStrategy()  {
        testRunner.setProperty(PutDropbox.FOLDER, MAIN_FOLDER);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, FAIL.getValue());

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);

        testRunner.clearTransferState();

        testRunner.enqueue(CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 1);
    }

    @Test
    void testUploadExistingFileReplaceStrategy()  {
        testRunner.setProperty(PutDropbox.FOLDER, MAIN_FOLDER);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, REPLACE.getValue());

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
        testRunner.clearTransferState();

        testRunner.enqueue(CHANGED_CONTENT);
        testRunner.run();
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
    }

    @Test
    void testUploadExistingFileIgnoreStrategy()  {
        testRunner.setProperty(PutDropbox.FOLDER, MAIN_FOLDER);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, IGNORE.getValue());

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
        testRunner.clearTransferState();

        testRunner.enqueue(CHANGED_CONTENT);
        testRunner.run();

        testRunner.assertTransferCount(PutDropbox.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutDropbox.REL_FAILURE, 0);
    }
}
