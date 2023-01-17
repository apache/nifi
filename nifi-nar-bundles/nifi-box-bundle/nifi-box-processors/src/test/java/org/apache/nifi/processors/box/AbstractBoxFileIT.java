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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Set the following constants before running:<br />
 * <br />
 * FOLDER_ID - The ID of a Folder the test can use to create files and sub-folders within.<br />
 * ACCOUNT_ID - The ID of the Account owning the Folder.<br />
 * APP_CONFIG_FILE - An App Settings Configuration JSON file.
 * Read nifi-nar-bundles/nifi-box-bundle/nifi-box-services/src/main/resources/docs/org.apache.nifi.box.controllerservices.JsonConfigBasedBoxClientService/additionalDetails.html for details.<br />
 * <br />
 * Created files and folders are cleaned up, but it's advisable to dedicate a folder for this test so that it can be cleaned up easily should the test fail to do so.
 */
public abstract class AbstractBoxFileIT<T extends Processor> {
    static final String FOLDER_ID = "";
    static final String ACCOUNT_ID = "";
    static final String APP_CONFIG_FILE = "";

    public static final String DEFAULT_FILE_CONTENT = "test_content";
    public static final String CHANGED_FILE_CONTENT = "changed_test_content";
    public static final String TEST_FILENAME = "test_filename.txt";
    public static final String MAIN_FOLDER_NAME = "main";

    protected T testSubject;
    protected TestRunner testRunner;

    protected BoxAPIConnection boxAPIConnection;

    protected String targetFolderName;
    protected String mainFolderId;

    protected abstract T createTestSubject();

    @BeforeEach
    protected void init() throws Exception {
        try (
            Reader reader = new FileReader(APP_CONFIG_FILE);
        ) {
            BoxConfig boxConfig = BoxConfig.readFrom(reader);
            boxAPIConnection = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);
            boxAPIConnection.asUser(ACCOUNT_ID);
        }

        targetFolderName = new BoxFolder(boxAPIConnection, FOLDER_ID).getInfo("name").getName();

        BoxFolder.Info mainFolderInfo = createFolder(MAIN_FOLDER_NAME, FOLDER_ID);
        mainFolderId = mainFolderInfo.getID();

        testSubject = createTestSubject();
        testRunner = createTestRunner();
    }

    @AfterEach
    protected void tearDown() {
        if (boxAPIConnection != null) {
            BoxFolder folder = new BoxFolder(boxAPIConnection, mainFolderId);
            folder.delete(true);
        }
    }

    protected TestRunner createTestRunner() throws Exception{
        TestRunner testRunner = TestRunners.newTestRunner(testSubject);

        BoxClientService boxClientService = mock(BoxClientService.class);
        doReturn(boxClientService.toString()).when(boxClientService).getIdentifier();
        doReturn(boxAPIConnection).when(boxClientService).getBoxApiConnection();

        testRunner.addControllerService(boxClientService.getIdentifier(), boxClientService);
        testRunner.enableControllerService(boxClientService);
        testRunner.setProperty(BoxClientService.BOX_CLIENT_SERVICE, boxClientService.getIdentifier());

        return testRunner;
    }

    protected BoxFolder.Info createFolder(String folderName, String parentFolderId) {
        BoxFolder parentFolder = new BoxFolder(boxAPIConnection, parentFolderId);
        BoxFolder.Info folderInfo = parentFolder.createFolder(folderName);
        return folderInfo;
    }

    protected BoxFile.Info createFileWithDefaultContent(String name, String folderId) {
        return createFile(name, folderId);
    }

    protected BoxFile.Info createFile(String name, String folderId) {
        BoxFolder folder = new BoxFolder(boxAPIConnection, folderId);

        BoxFile.Info fileInfo = folder.uploadFile(new ByteArrayInputStream(DEFAULT_FILE_CONTENT.getBytes(UTF_8)), name);

        return fileInfo;
    }

    protected Set<String> getCheckedAttributeNames() {
        Set<String> checkedAttributeNames = Arrays.stream(BoxFlowFileAttribute.values())
            .map(BoxFlowFileAttribute::getName)
            .collect(Collectors.toSet());

        return checkedAttributeNames;
    }
}
