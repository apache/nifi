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
package org.apache.nifi.box.controllerservices;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFolder;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.mockito.Mockito.mock;

/**
 * Set the following constants before running:<br />
 * <br />
 * FOLDER_ID - The ID of a Folder the test can use to retrieve metadata for.<br />
 * ACCOUNT_ID - The ID of the Account owning the Folder.<br />
 * APP_CONFIG_FILE - An App Settings Configuration JSON file.
 * Read nifi-nar-bundles/nifi-box-bundle/nifi-box-services/src/main/resources/docs/org.apache.nifi.box.controllerservices.JsonConfigBasedBoxClientService/additionalDetails.html for details.<br />
 */
public class JsonConfigBasedBoxClientServiceIT {
    static final String FOLDER_ID = "";
    static final String ACCOUNT_ID = "";
    static final String APP_CONFIG_FILE = "";

    protected JsonConfigBasedBoxClientService testSubject;
    protected TestRunner testRunner;

    @BeforeEach
    protected void init() throws Exception {
        testSubject = new JsonConfigBasedBoxClientService();

        Processor dummyProcessor = mock(Processor.class);
        testRunner = TestRunners.newTestRunner(dummyProcessor);

        testRunner.addControllerService(testSubject.getClass().getSimpleName(), testSubject);
    }

    @Test
    void testWithAppConfigJson() throws Exception {
        // GIVEN
        String appConfigJson = new String(Files.readAllBytes(Paths.get(APP_CONFIG_FILE)));

        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, ACCOUNT_ID);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, appConfigJson);
        testRunner.enableControllerService(testSubject);

        BoxAPIConnection boxAPIConnection = testSubject.getBoxApiConnection();

        // WHEN
        // THEN
        new BoxFolder(boxAPIConnection, FOLDER_ID).getInfo("name").getName();
    }

    @Test
    void testWithAppConfigFile() throws Exception {
        // GIVEN
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, ACCOUNT_ID);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_FILE, APP_CONFIG_FILE);
        testRunner.enableControllerService(testSubject);

        BoxAPIConnection boxAPIConnection = testSubject.getBoxApiConnection();

        // WHEN
        // THEN
        new BoxFolder(boxAPIConnection, FOLDER_ID).getInfo("name").getName();
    }
}
