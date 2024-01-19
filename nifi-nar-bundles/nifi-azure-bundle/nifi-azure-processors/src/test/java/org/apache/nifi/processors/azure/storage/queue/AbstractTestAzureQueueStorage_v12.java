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
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsType;
import org.apache.nifi.util.TestRunner;

public abstract class AbstractTestAzureQueueStorage_v12 {
    public static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";
    protected TestRunner runner;
    protected AzureStorageCredentialsService_v12 credentialsService = new AzureStorageCredentialsControllerService_v12();

    protected void setupStorageCredentialsService() throws InitializationException {
        runner.addControllerService(CREDENTIALS_SERVICE_IDENTIFIER, credentialsService);
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_NAME, "account-name");
        runner.setProperty(credentialsService, AzureStorageUtils.CREDENTIALS_TYPE, AzureStorageCredentialsType.ACCOUNT_KEY);
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY, "account-key");
    }
}
