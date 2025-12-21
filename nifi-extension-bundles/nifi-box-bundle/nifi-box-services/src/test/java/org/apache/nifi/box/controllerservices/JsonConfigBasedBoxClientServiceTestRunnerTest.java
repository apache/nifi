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

import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonConfigBasedBoxClientServiceTestRunnerTest {
    private JsonConfigBasedBoxClientService testSubject;
    private TestRunner testRunner;

    @BeforeEach
    void setUp() throws Exception {
        testSubject = new JsonConfigBasedBoxClientService();
        testRunner = TestRunners.newTestRunner(NoOpProcessor.class);
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
        // App Actor is Impersonated User by default.
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

    @Test
    void validWhenAppActorIsServiceAccount() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_ACTOR, BoxAppActor.SERVICE_ACCOUNT);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertValid(testSubject);
    }

    @Test
    void invalidWhenAppActorIsServiceAccountAndAccountIdIsSet() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_ACTOR, BoxAppActor.SERVICE_ACCOUNT);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertValid(testSubject);
    }

    @Test
    void validWhenAppActorIsImpersonatedUserAndAccountIdIsSet() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_ACTOR, BoxAppActor.IMPERSONATED_USER);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.ACCOUNT_ID, "account_id");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertValid(testSubject);
    }

    @Test
    void invalidWhenAppActorIsImpersonatedUserAndAccountIdIsMissing() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_ACTOR, BoxAppActor.IMPERSONATED_USER);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.assertNotValid(testSubject);
    }

    @Test
    void validWhenCustomTimeoutsAreSet() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_ACTOR, BoxAppActor.SERVICE_ACCOUNT);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.CONNECT_TIMEOUT, "1 min");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.READ_TIMEOUT, "10 sec");
        testRunner.assertValid(testSubject);
    }

    @Test
    void invalidWhenTimeoutsAreNotTimePeriods() {
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_ACTOR, BoxAppActor.SERVICE_ACCOUNT);
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.APP_CONFIG_JSON, "{}");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.CONNECT_TIMEOUT, "not_a_time_period");
        testRunner.setProperty(testSubject, JsonConfigBasedBoxClientService.READ_TIMEOUT, "1234");
        testRunner.assertNotValid(testSubject);
    }

    @Test
    void testMigration() {
        final Map<String, String> propertyValues = Map.of(
                JsonConfigBasedBoxClientService.ACCOUNT_ID.getName(), "account_id",
                JsonConfigBasedBoxClientService.CONNECT_TIMEOUT.getName(), "1 min",
                JsonConfigBasedBoxClientService.READ_TIMEOUT.getName(), "1234"
        );

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        final JsonConfigBasedBoxClientService jsonConfigBasedBoxClientService = new JsonConfigBasedBoxClientService();
        jsonConfigBasedBoxClientService.migrateProperties(configuration);

        Map<String, String> expected = Map.ofEntries(
                Map.entry("box-account-id", JsonConfigBasedBoxClientService.ACCOUNT_ID.getName()),
                Map.entry("app-config-file", JsonConfigBasedBoxClientService.APP_CONFIG_FILE.getName()),
                Map.entry("app-config-json", JsonConfigBasedBoxClientService.APP_CONFIG_JSON.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }
}
