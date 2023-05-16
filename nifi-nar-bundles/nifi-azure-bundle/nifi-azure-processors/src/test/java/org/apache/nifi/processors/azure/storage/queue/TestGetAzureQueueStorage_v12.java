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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGetAzureQueueStorage_v12 extends AbstractTestAzureQueueStorage_v12 {
    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(GetAzureQueueStorage_v12.class);
        setupStorageCredentialsService();
        runner.enableControllerService(credentialsService);
        runner.setProperty(GetAzureQueueStorage_v12.STORAGE_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_IDENTIFIER);
        runner.setProperty(GetAzureQueueStorage_v12.QUEUE_NAME, "queue");
        runner.setProperty(GetAzureQueueStorage_v12.MESSAGE_BATCH_SIZE, "10");
    }

    @Test
    public void testValidVisibilityTimeout() {
        runner.setProperty(GetAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "10 secs");

        runner.assertValid();
    }

    @Test
    public void testInvalidVisibilityTimeoutZeroSecs() {
        runner.setProperty(GetAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "0 secs");

        runner.assertNotValid();
    }

    @Test
    public void testInvalidVisibilityTimeoutMoreThanSevenDays() {
        runner.setProperty(GetAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "8 days");

        runner.assertNotValid();
    }

    @Test
    public void testValidRequestTimeout() {
        runner.setProperty(GetAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "10 secs");
        runner.setProperty(GetAzureQueueStorage_v12.REQUEST_TIMEOUT, "15 secs");

        runner.assertValid();
    }

    @Test
    public void testInvalidRequestTimeoutZeroSecs() {
        runner.setProperty(GetAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "10 secs");
        runner.setProperty(GetAzureQueueStorage_v12.REQUEST_TIMEOUT, "0 secs");

        runner.assertNotValid();
    }

    @Test
    public void testInvalidRequestTimeoutMoreThanThirtySecs() {
        runner.setProperty(GetAzureQueueStorage_v12.VISIBILITY_TIMEOUT, "10 secs");
        runner.setProperty(GetAzureQueueStorage_v12.REQUEST_TIMEOUT, "31 secs");

        runner.assertNotValid();
    }
}
