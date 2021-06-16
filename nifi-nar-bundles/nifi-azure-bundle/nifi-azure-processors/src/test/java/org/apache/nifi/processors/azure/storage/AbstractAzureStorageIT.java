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
package org.apache.nifi.processors.azure.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerService;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Before;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.fail;

public abstract class AbstractAzureStorageIT {

    private static final Properties CONFIG;

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.PROPERTIES";

    static {
        CONFIG = new Properties();
        try {
            final FileInputStream fis = new FileInputStream(CREDENTIALS_FILE);
            try {
                CONFIG.load(fis);
            } catch (IOException e) {
                fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
            } finally {
                FileUtils.closeQuietly(fis);
            }
        } catch (FileNotFoundException e) {
            fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
        }
    }

    protected static String getAccountName() {
        return CONFIG.getProperty("accountName");
    }

    protected static String getAccountKey() {
        return CONFIG.getProperty("accountKey");
    }

    protected TestRunner runner;

    @Before
    public void setUpAzureStorageIT() throws Exception {
        runner = TestRunners.newTestRunner(getProcessorClass());

        setUpCredentials();
    }

    protected void setUpCredentials() throws Exception {
        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, getAccountKey());
    }

    protected abstract Class<? extends Processor> getProcessorClass();

    protected CloudStorageAccount getStorageAccount() throws Exception {
        StorageCredentials storageCredentials = new StorageCredentialsAccountAndKey(getAccountName(), getAccountKey());
        return new CloudStorageAccount(storageCredentials, true);
    }

    protected void configureCredentialsService() throws Exception {
        runner.removeProperty(AzureStorageUtils.ACCOUNT_NAME);
        runner.removeProperty(AzureStorageUtils.ACCOUNT_KEY);

        AzureStorageCredentialsService credentialsService = new AzureStorageCredentialsControllerService();

        runner.addControllerService("credentials-service", credentialsService);

        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_NAME, getAccountName());
        runner.setProperty(credentialsService, AzureStorageUtils.ACCOUNT_KEY, getAccountKey());

        runner.assertValid(credentialsService);

        runner.enableControllerService(credentialsService);

        runner.setProperty(AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE, credentialsService.getIdentifier());
    }
}
