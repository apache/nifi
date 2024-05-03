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

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.GetMetadataErrorException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.services.dropbox.StandardDropboxCredentialService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Set the following constants before running: <br/>
 * <br/> APPLICATION_KEY - App Key of your app
 * <br/> APPLICATION_SECRET - Your App Secret of your app
 * <br/> ACCESS_TOKEN - Access Token generated for your app
 * <br/> REFRESH_TOKEN - Refresh Token generated for your app
 * <br/><br/>
 * For App Key, App Secret, Access Token and Refresh Token generation please check controller service's Additional Details page.<br/><br/>
 * NOTE: Since the integration test creates the test files you need "files.content.write" permission besides the "files.content.read"
 * permission mentioned in controller service's Additional Details page.<br/>
 *
 * <br/> Created files and folders are cleaned up, but it's advisable to dedicate a folder for this test so that it can
 * be cleaned up easily should the test fail to do so. <br/>
 * <br/> WARNING: The creation of a file is not a synchronized operation, may need to adjust tests accordingly!
 */
public abstract class AbstractDropboxIT<T extends Processor> {

    public static final String APP_KEY = "";
    public static final String APP_SECRET = "";
    public static final String ACCESS_TOKEN = "";
    public static final String REFRESH_TOKEN = "";

    public static final String MAIN_FOLDER = "/testFolder";
    protected T testSubject;
    protected TestRunner testRunner;
    protected String mainFolderId;

    private DbxClientV2 client;

    @AfterEach
    public void teardown() throws Exception {
        deleteFolderIfExists(MAIN_FOLDER);
    }

    protected abstract T createTestSubject();

    @BeforeEach
    protected void init() throws Exception {
        testSubject = createTestSubject();
        testRunner = createTestRunner();
        DbxCredential credential =
                new DbxCredential(ACCESS_TOKEN, -1L, REFRESH_TOKEN, APP_KEY, APP_SECRET);
        DbxRequestConfig config = new DbxRequestConfig("nifi");
        client = new DbxClientV2(config, credential);
        mainFolderId = createFolder(MAIN_FOLDER);
    }

    protected TestRunner createTestRunner() throws Exception {
        TestRunner testRunner = TestRunners.newTestRunner(testSubject);

        StandardDropboxCredentialService controllerService = new StandardDropboxCredentialService();
        testRunner.addControllerService("dropbox_credential_provider_service", controllerService);
        testRunner.setProperty(controllerService, StandardDropboxCredentialService.APP_KEY, APP_KEY);
        testRunner.setProperty(controllerService, StandardDropboxCredentialService.APP_SECRET, APP_SECRET);
        testRunner.setProperty(controllerService, StandardDropboxCredentialService.ACCESS_TOKEN, ACCESS_TOKEN);
        testRunner.setProperty(controllerService, StandardDropboxCredentialService.REFRESH_TOKEN, REFRESH_TOKEN);
        testRunner.enableControllerService(controllerService);
        testRunner.setProperty(ListDropbox.CREDENTIAL_SERVICE, "dropbox_credential_provider_service");

        return testRunner;
    }

    protected void deleteFolderIfExists(String path) throws Exception {
        if (folderExists(path)) {
            client.files().deleteV2(path);
        }
    }

    protected FileMetadata createFile(String folder, String filename, String fileContent) throws Exception {
        ByteArrayInputStream content = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
        return client.files().upload(folder.equals("/") ?  "/" + filename : folder + "/" + filename).uploadAndFinish(content);
    }

    private String createFolder(String path) throws Exception {
        if (folderExists(path)) {
            deleteFolder(path);
        }
        return client.files().createFolderV2(path).getMetadata().getId();
    }

    private void deleteFolder(String path) throws Exception {
        client.files().deleteV2(path);
    }

    private boolean folderExists(String path) throws Exception {
        try {
            return client.files().getMetadata(path) != null;
        } catch (GetMetadataErrorException e) {
            if (e.errorValue.isPath() && e.errorValue.getPathValue().isNotFound()) {
                return false;
            }
            throw e;
        }
    }
}