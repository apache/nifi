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

import com.dropbox.core.v2.files.FileMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FetchDropboxIT extends AbstractDropboxIT<FetchDropbox> {

    private static final String DEFAULT_FILE_CONTENT = "test_file_content1";

    @BeforeEach
    public void init() throws Exception {
        super.init();
    }

    @Override
    protected FetchDropbox createTestSubject() {
        return new FetchDropbox();
    }

    @Test
    void testFetchFileById() throws Exception {
        FileMetadata file = createFile("/testFolder", "test_file1", DEFAULT_FILE_CONTENT);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("dropbox.id", file.getId());

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchDropbox.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(DEFAULT_FILE_CONTENT);
    }

    @Test
    void testFetchFileByPath() throws Exception {
        createFile("/testFolder", "test_file1", DEFAULT_FILE_CONTENT);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("path", "/testFolder");
        inputFlowFileAttributes.put("filename", "test_file1");

        testRunner.setProperty(FetchDropbox.FILE, "${path}/${filename}");

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertContentEquals(DEFAULT_FILE_CONTENT);
    }

    @Test
    void testFetchFileFromRootByPath() throws Exception {
        createFile("/", "test_file1", DEFAULT_FILE_CONTENT);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("path", "/");
        inputFlowFileAttributes.put("filename", "test_file1");

        testRunner.setProperty(FetchDropbox.FILE, "${path}/${filename}");

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchDropbox.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertContentEquals(DEFAULT_FILE_CONTENT);
    }

    @Test
    void testFetchingFolderFails() {
        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("path", "/testFolder");

        testRunner.setProperty(FetchDropbox.FILE, "${path}");

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchDropbox.REL_FAILURE, 1);
        testRunner.assertTransferCount(FetchDropbox.REL_SUCCESS, 0);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("error.message", "Exception in 2/files/download: {\".tag\":\"path\",\"path\":\"not_file\"}");
    }

    @Test
    void testFetchingFileByWrongIdFails() {
        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put("dropbox.id", "id:missing");

        testRunner.enqueue("unimportant_data", inputFlowFileAttributes);
        testRunner.run();

        testRunner.assertTransferCount(FetchDropbox.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchDropbox.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("error.message", "Exception in 2/files/download: {\".tag\":\"path\",\"path\":\"not_found\"}");
    }
}
