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
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Set the following constants before running:<br />
 * <br />
 * CREDENTIAL_JSON_FILE_PATH - A Service Account credentials JSON file.<br />
 * SHARED_FOLDER_ID - The ID of a Folder that is shared with the Service Account. The test will create files and sub-folders within this folder.<br />
 * <br />
 * Created files and folders are cleaned up, but it's advisable to dedicate a folder for this test so that it can be cleaned up easily should the test fail to do so.
 * <br /><br />
 * WARNING: The creation of a file is not a synchronized operation so tests may fail because the processor may not list all of them.
 */
public class ListGoogleDriveIT {
    public static final String CREDENTIAL_JSON_FILE_PATH = "";
    public static final String SHARED_FOLDER_ID = "";

    public static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    private TestRunner testRunner;

    private Drive driveService;

    private String mainFolderId;

    @BeforeEach
    public void init() throws Exception {
        ListGoogleDrive testSubject = new ListGoogleDrive();

        testRunner = TestRunners.newTestRunner(testSubject);

        GCPCredentialsControllerService gcpCredentialsControllerService = new GCPCredentialsControllerService();
        testRunner.addControllerService("gcp_credentials_provider_service", gcpCredentialsControllerService);
        testRunner.setProperty(gcpCredentialsControllerService, CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE, CREDENTIAL_JSON_FILE_PATH);
        testRunner.enableControllerService(gcpCredentialsControllerService);
        testRunner.setProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE, "gcp_credentials_provider_service");

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

        File file = createFolder("main", SHARED_FOLDER_ID);

        mainFolderId = file.getId();
        testRunner.setProperty(ListGoogleDrive.FOLDER_ID, mainFolderId);
    }

    @AfterEach
    void tearDown() throws IOException {
        driveService.files()
                .delete(mainFolderId)
                .execute();
    }

    @Test
    void listFilesFrom3LayerDeepDirectoryTree() throws Exception {
        // GIVEN
        File main_sub1 = createFolder("main_sub1", mainFolderId);
        File main_sub2 = createFolder("main_sub2", mainFolderId);

        File main_sub1_sub1 = createFolder("main_sub1_sub1", main_sub1.getId());

        createFile("main_file1", mainFolderId);
        createFile("main_file2", mainFolderId);
        createFile("main_file3", mainFolderId);

        createFile("main_sub1_file1", main_sub1.getId());

        createFile("main_sub2_file1", main_sub2.getId());
        createFile("main_sub2_file2", main_sub2.getId());

        createFile("main_sub1_sub1_file1", main_sub1_sub1.getId());
        createFile("main_sub1_sub1_file2", main_sub1_sub1.getId());
        createFile("main_sub1_sub1_file3", main_sub1_sub1.getId());

        Set<String> expectedFileNames = new HashSet<>(Arrays.asList(
                "main_file1", "main_file2", "main_file3",
                "main_sub1_file1",
                "main_sub2_file1", "main_sub2_file2",
                "main_sub1_sub1_file1", "main_sub1_sub1_file2", "main_sub1_sub1_file3"
        ));

        // The creation of the files are not (completely) synchronized.
        Thread.sleep(2000);

        // WHEN
        testRunner.run();

        // THEN
        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        Set<String> actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);

        // Next, list a sub folder, non-recursively this time. (Changing these properties will clear the Processor state as well
        //  so all files are eligible for listing again.)

        // GIVEN
        testRunner.clearTransferState();

        expectedFileNames = new HashSet<>(Arrays.asList(
                "main_sub1_file1"
        ));

        // WHEN
        testRunner.setProperty(ListGoogleDrive.FOLDER_ID, main_sub1.getId());
        testRunner.setProperty(ListGoogleDrive.RECURSIVE_SEARCH, "false");
        testRunner.run();

        // THEN
        successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);
    }

    @Test
    void doNotListTooYoungFilesWhenMinAgeIsSet() throws Exception {
        // GIVEN
        testRunner.setProperty(ListGoogleDrive.MIN_AGE, "15 s");

        createFile("main_file", mainFolderId);

        // Make sure the file 'arrives' and could be listed
        Thread.sleep(5000);

        // WHEN
        testRunner.run();

        // THEN
        List<MockFlowFile> successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        Set<String> actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), actualFileNames);

        // Next, wait for another 10+ seconds for MIN_AGE to expire then list again

        // GIVEN
        Thread.sleep(10000);

        Set<String> expectedFileNames = new HashSet<>(Arrays.asList(
                "main_file"
        ));

        // WHEN
        testRunner.run();

        // THEN
        successFlowFiles = testRunner.getFlowFilesForRelationship(ListGoogleDrive.REL_SUCCESS);

        actualFileNames = successFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("filename"))
                .collect(Collectors.toSet());

        assertEquals(expectedFileNames, actualFileNames);
    }

    private File createFolder(String folderName, String... parentFolderIds) throws IOException {
        File fileMetaData = new File();
        fileMetaData.setName(folderName);

        if (parentFolderIds != null) {
            fileMetaData.setParents(Arrays.asList(parentFolderIds));
        }

        fileMetaData.setMimeType("application/vnd.google-apps.folder");

        Drive.Files.Create create = driveService.files()
                .create(fileMetaData)
                .setFields("id");

        File file = create.execute();

        return file;
    }

    private File createFile(String name, String... folderIds) throws IOException {
        File fileMetadata = new File();
        fileMetadata.setName(name);
        fileMetadata.setParents(Arrays.asList(folderIds));

        AbstractInputStreamContent content = new ByteArrayContent("text/plain", "test_content".getBytes(StandardCharsets.UTF_8));

        Drive.Files.Create create = driveService.files()
                .create(fileMetadata, content)
                .setFields("id, modifiedTime");

        File file = create.execute();

        return file;
    }

}
