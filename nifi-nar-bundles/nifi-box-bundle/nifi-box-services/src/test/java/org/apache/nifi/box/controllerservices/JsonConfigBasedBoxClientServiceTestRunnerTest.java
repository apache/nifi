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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

public class JsonConfigBasedBoxClientServiceTestRunnerTest {
    private JsonConfigBasedBoxClientService testSubject;
    private TestRunner testRunner;

    @BeforeEach
    void setUp() throws Exception {
        testSubject = new JsonConfigBasedBoxClientService();

        Processor dummyProcessor = mock(Processor.class);
        testRunner = TestRunners.newTestRunner(dummyProcessor);

        testRunner.addControllerService(testSubject.getClass().getSimpleName(), testSubject);
    }

    @Test
    void validWhenAppConfigFileIsSet() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_FILE, "pom.xml");
        testRunner.assertValid(testSubject);
    }

    @Test
    void validWhenAppConfigJsonIsSet() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertValid(testSubject);
    }

    @Test
    void invalidWhenAccountIdIsMissing() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertNotValid(testSubject);
    }

    @Test
    void invalidWhenAppConfigFileAndAppConfigJsonIsMissing() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.assertNotValid(testSubject);
    }

    @Test
    void invalidWhenAppConfigFileDoesNotExist() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_FILE, "doesnotexist.xml");
        testRunner.assertNotValid(testSubject);
    }

    @Test
    void invalidWhenBothAppConfigFileAndAppConfigJsonIsSet() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_FILE, "pom.xml");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertNotValid(testSubject);
    }

    @Test
    void invalidWhenAppConfigJsonIsNotValid() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "not_valid_json_string");
        testRunner.assertNotValid(testSubject);
    }
}
