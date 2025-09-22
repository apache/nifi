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
package org.apache.nifi.processors.azure.storage.queue;

import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPutAzureQueueStorage_v12 extends AbstractTestAzureQueueStorage_v12 {
    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PutAzureQueueStorage_v12.class);
        setupStorageCredentialsService();
        runner.enableControllerService(credentialsService);
        runner.setProperty(PutAzureQueueStorage_v12.STORAGE_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_IDENTIFIER);
        runner.setProperty(PutAzureQueueStorage_v12.QUEUE_NAME, "queue");
    }

    @Test
    public void testValidVisibilityTimeout() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "10 secs");

        runner.assertValid();
    }

    @Test
    public void testInvalidVisibilityTimeoutZeroSecs() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "0 secs");

        runner.assertNotValid();
    }

    @Test
    public void testInvalidVisibilityTimeoutMoreThanSevenDays() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "8 days");

        runner.assertNotValid();
    }

    @Test
    public void testValidTTL() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");

        runner.assertValid();
    }

    @Test
    public void testInvalidTTLZeroSecs() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "0 secs");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");

        runner.assertNotValid();
    }

    @Test
    public void testValidRequestTimeout() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");
        runner.setProperty(PutAzureQueueStorage_v12.REQUEST_TIMEOUT, "15 secs");

        runner.assertValid();
    }

    @Test
    public void testInvalidRequestTimeoutZeroSecs() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");
        runner.setProperty(PutAzureQueueStorage_v12.REQUEST_TIMEOUT, "0 secs");

        runner.assertNotValid();
    }

    @Test
    public void testInvalidRequestTimeoutMoreThanThirtySecs() {
        runner.setProperty(PutAzureQueueStorage_v12.MESSAGE_TIME_TO_LIVE, "7 days");
        runner.setProperty(PutAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "30 secs");
        runner.setProperty(PutAzureQueueStorage_v12.REQUEST_TIMEOUT, "31 secs");

        runner.assertNotValid();
    }

    @Test
    void testMigration() {
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expected = Map.of(
                AzureStorageUtils.STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME, GetAzureQueueStorage_v12.ENDPOINT_SUFFIX.getName()
        );

        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }
}
