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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Set the following constants before running:<br />
 * <br />
 * CREDENTIAL_JSON_FILE_PATH - A Service Account credentials JSON file.<br />
 * SHARED_FOLDER_ID - The ID of a Folder that is shared with the Service Account. The test will create files and sub-folders within this folder.<br />
 * <br />
 * Created files and folders are cleaned up, but it's advisable to dedicate a folder for this test so that it can be cleaned up easily should the test fail to do so.
 * In case your shared folder is located on a shared drive, give Service Account "Manager" permission on the shared drive to make it capable of deleting the created test files.
 * <br /><br />
 * WARNING: The creation of a file is not a synchronized operation, may need to adjust tests accordingly!
 */
public abstract class AbstractGoogleDriveIT<T extends GoogleDriveTrait & Processor> {
    protected static final String SHARED_FOLDER_ID = "";
    protected static final String DEFAULT_FILE_CONTENT = "test_content";

    protected static final String LARGE_FILE_CONTENT = StringUtils.repeat("a", 355 * 1024);

    private static final String CREDENTIAL_JSON_FILE_PATH = "";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    protected T testSubject;
    protected TestRunner testRunner;

    protected Drive driveService;

    protected String mainFolderId;

    protected abstract T createTestSubject();

    @BeforeEach
    protected void init() throws Exception {
        testSubject = createTestSubject();
        testRunner = createTestRunner();

        NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        driveService = new Drive.Builder(
                httpTransport,
                JSON_FACTORY,
                testSubject.getHttpCredentialsAdapter(
                        testRunner.getProcessContext(),
                        DriveScopes.all()
                )
        )
                .setApplicationName(this.getClass().getSimpleName())
                .build();

        File mainFolder = createFolder("main", SHARED_FOLDER_ID);
        mainFolderId = mainFolder.getId();
    }

    @AfterEach
    protected void tearDown() throws Exception {
        if (driveService != null) {
            driveService.files()
                    .delete(mainFolderId)
                    .setSupportsAllDrives(true)
                    .execute();
        }
    }

    protected TestRunner createTestRunner() throws Exception {
        TestRunner testRunner = TestRunners.newTestRunner(testSubject);

        GCPCredentialsControllerService gcpCredentialsControllerService = new GCPCredentialsControllerService();
        testRunner.addControllerService("gcp_credentials_provider_service", gcpCredentialsControllerService);

        testRunner.setProperty(gcpCredentialsControllerService, CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE, CREDENTIAL_JSON_FILE_PATH);
        testRunner.enableControllerService(gcpCredentialsControllerService);
        testRunner.setProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE, "gcp_credentials_provider_service");

        return testRunner;
    }

    protected File createFolder(String folderName, String... parentFolderIds) throws IOException {
        File fileMetaData = new File();
        fileMetaData.setName(folderName);

        if (parentFolderIds != null) {
            fileMetaData.setParents(Arrays.asList(parentFolderIds));
        }

        fileMetaData.setMimeType("application/vnd.google-apps.folder");

        Drive.Files.Create create = driveService.files()
                .create(fileMetaData)
                .setSupportsAllDrives(true)
                .setFields("id");

        File file = create.execute();

        return file;
    }

    protected File createFileWithDefaultContent(String name, String... folderIds) throws IOException {
        return createFile(name, DEFAULT_FILE_CONTENT, folderIds);
    }

    protected File createFile(String name, String fileContent, String... folderIds) throws IOException {
        File fileMetadata = new File();
        fileMetadata.setName(name);
        fileMetadata.setParents(Arrays.asList(folderIds));

        AbstractInputStreamContent content = new ByteArrayContent("text/plain", fileContent.getBytes(StandardCharsets.UTF_8));

        Drive.Files.Create create = driveService.files()
                .create(fileMetadata, content)
                .setSupportsAllDrives(true)
                .setFields("id, name, modifiedTime, createdTime");

        File file = create.execute();

        return file;
    }

    public TestRunner getTestRunner() {
        return testRunner;
    }


}
